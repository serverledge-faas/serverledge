package test

import (
	"context"
	"flag"
	"fmt"
	"github.com/serverledge-faas/serverledge/internal/config"
	"log"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/labstack/echo/v4"
	"github.com/serverledge-faas/serverledge/internal/api"
	"github.com/serverledge-faas/serverledge/internal/metrics"
	"github.com/serverledge-faas/serverledge/internal/node"
	"github.com/serverledge-faas/serverledge/internal/registration"
	"github.com/serverledge-faas/serverledge/internal/scheduling"
	"github.com/serverledge-faas/serverledge/internal/workflow"
	u "github.com/serverledge-faas/serverledge/utils"
	"google.golang.org/grpc/codes"
)

const HOST = "127.0.0.1"
const PORT = 1323

func getShell() string {
	if IsWindows() {
		return "powershell.exe"
	} else {
		return "/bin/sh"
	}
}

func getShellExt() string {
	if IsWindows() {
		return ".bat"
	} else {
		return ".sh"
	}
}

func testStartServerledge(isInCloud bool, outboundIp string) *echo.Echo {
	//setting up cache parameters
	api.CacheSetup()
	schedulingPolicy := &scheduling.DefaultLocalPolicy{}
	// register to etcd, this way server is visible to the others under a given local area
	myArea := config.GetString(config.REGISTRY_AREA, "ROME")
	node.LocalNode = node.NewIdentifier(myArea)

	err := registration.RegisterNode()
	if err != nil {
		log.Fatal(err)
	}

	metrics.Init()

	// Workflow offloading policy
	workflow.CreateOffloadingPolicy()

	e := echo.New()

	// Register a signal handler to cleanup things on termination
	api.RegisterTerminationHandler(e)

	go scheduling.Run(schedulingPolicy)

	if !isInCloud {
		err = registration.StartMonitoring()
		if err != nil {
			log.Fatal(err)
		}
	}
	// needed: if you call a function composition, internally will invoke each function
	go api.StartAPIServer(e)
	return e

}

// current dir is ./serverledge/internal/workflow
func TestMain(m *testing.M) {
	flag.Parse() // Parsing the test flags. Needed to ensure that the -short flag is parsed, so testing.Short() returns a nonNil bool

	outboundIp, err := u.GetOutboundIp()
	if err != nil || outboundIp == nil {
		log.Fatalf("test cannot be executed without internet connection")
	}

	// TODO: avoid full setup if testing.Short()

	echoServer, ok := setupServerledge(outboundIp.String())
	if ok != nil {
		fmt.Printf("failed to initialize serverledgde: %v\n", ok)
		os.Exit(int(codes.Internal))
	}

	// run all test independently
	code := m.Run()

	// tear down containers in order
	err = teardownServerledge(echoServer)
	if err != nil {
		fmt.Printf("failed to remove serverledgde: %v\n", err)
	}
	os.Exit(code)
}

// startReliably can start the containers, or restart them if needed
func startReliably(startScript string) error {
	cmd := exec.CommandContext(context.Background(), getShell(), startScript)
	err := cmd.Run()
	if err != nil {
		log.Fatalf("failed to start (%s): %v", startScript, err)
	}
	return err
}

// run the bash script to initialize serverledge
func setupServerledge(outboundIp string) (*echo.Echo, error) {
	echoServer := testStartServerledge(false, outboundIp)
	return echoServer, nil
}

// run the bash script to stop serverledge
func teardownServerledge(e *echo.Echo) error {

	node.ShutdownAllContainers()

	node.StopJanitor()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	errEcho := e.Shutdown(ctx)

	errRegistry := registration.Deregister()
	return u.ReturnNonNilErr(errEcho, errRegistry)
}
