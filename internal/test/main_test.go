package test

import (
	"context"
	"fmt"
	"github.com/grussorusso/serverledge/internal/api"
	"github.com/grussorusso/serverledge/internal/config"
	"github.com/grussorusso/serverledge/internal/metrics"
	"github.com/grussorusso/serverledge/internal/node"
	"github.com/grussorusso/serverledge/internal/registration"
	"github.com/grussorusso/serverledge/internal/scheduling"
	u "github.com/grussorusso/serverledge/utils"
	"github.com/labstack/echo/v4"
	"google.golang.org/grpc/codes"
	"log"
	"os"
	"os/exec"
	"testing"
	"time"
)

const HOST = "127.0.0.1"
const PORT = 1323
const AREA = "ROME"

var IntegrationTest bool

func testStartServerledge(isInCloud bool) (*registration.Registry, *echo.Echo) {
	//setting up cache parameters
	api.CacheSetup()
	schedulingPolicy := &scheduling.DefaultLocalPolicy{}
	// register to etcd, this way server is visible to the others under a given local area
	registry := new(registration.Registry)
	if isInCloud {
		registry.Area = "cloud/" + AREA
	} else {
		registry.Area = AREA
	}
	// before register checkout other servers into the local area
	//todo use this info later on; future work with active remote server selection
	_, err := registry.GetAll(true)
	if err != nil {
		log.Fatal(err)
	}

	ip := config.GetString(config.API_IP, u.GetIpAddress().String())
	url := fmt.Sprintf("http://%s:%d", ip, PORT)
	myKey, err := registry.RegisterToEtcd(url)
	if err != nil {
		log.Fatal(err)
	}

	node.NodeIdentifier = myKey

	go metrics.Init()

	e := echo.New()

	// Register a signal handler to cleanup things on termination
	api.RegisterTerminationHandler(registry, e)

	go scheduling.Run(schedulingPolicy)

	if !isInCloud {
		err = registration.InitEdgeMonitoring(registry)
		if err != nil {
			log.Fatal(err)
		}
	}
	// needed: if you call a function composition, internally will invoke each function
	go api.StartAPIServer(e)
	return registry, e

}

// current dir is ./serverledge/internal/fc
func TestMain(m *testing.M) {
	_, Experiment = os.LookupEnv("EXPERIMENT")
	_, IntegrationTest = os.LookupEnv("INTEGRATION")
	// spin up container with serverledge infrastructure
	if IntegrationTest {

		registry, echoServer, ok := setupServerledge()
		if ok != nil {
			fmt.Printf("failed to initialize serverledgde: %v\n", ok)
			os.Exit(int(codes.Internal))
		}

		// run all test independently
		code := m.Run()
		// tear down containers in order
		err := teardownServerledge(registry, echoServer)
		if err != nil {
			fmt.Printf("failed to remove serverledgde: %v\n", err)
			os.Exit(int(codes.Internal))
		}
		os.Exit(code)
	} else {
		code := m.Run()
		os.Exit(code)
	}
}

// startReliably can start the containers, or restart them if needed
func startReliably(startScript string, stopScript string, msg string) error {
	cmd := exec.CommandContext(context.Background(), "/bin/sh", startScript)
	err := cmd.Run()
	if err != nil {
		antiCmd := exec.CommandContext(context.Background(), "/bin/sh", stopScript)
		err = antiCmd.Run()
		if err != nil {
			return fmt.Errorf("stopping of %s failed", msg)
		}
		cmd = exec.CommandContext(context.Background(), "/bin/sh", startScript)
		err = cmd.Run()
	}
	if err == nil {
		fmt.Printf("%s started\n", msg)
	}
	return err
}

// run the bash script to initialize serverledge
func setupServerledge() (*registration.Registry, *echo.Echo, error) {
	err1 := startReliably("../../scripts/start-etcd.sh", "../../scripts/stop-etcd.sh", "ETCD")
	registry, echoServer := testStartServerledge(false)
	return registry, echoServer, u.ReturnNonNilErr(err1)
}

// run the bash script to stop serverledge
func teardownServerledge(registry *registration.Registry, e *echo.Echo) error {
	cmd1 := exec.CommandContext(context.Background(), "/bin/sh", "../../scripts/remove-etcd.sh")

	node.Resources.Lock()
	nContainers := len(node.Resources.ContainerPools)
	fmt.Printf("Terminating all %d containers...\n", nContainers)
	node.Resources.Unlock()

	node.ShutdownAllContainers()

	//stop container janitor
	node.StopJanitor()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	errEcho := e.Shutdown(ctx)

	errRegistry := registry.Deregister()
	err1 := cmd1.Run()
	fmt.Println("ETCD removed")
	return u.ReturnNonNilErr(errEcho, errRegistry, err1)
}
