package test

import (
	"context"
	"flag"
	"fmt"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"
	"log"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/serverledge-faas/serverledge/internal/config"

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
	node.LocalNode = node.NewRandomIdentifier(myArea)

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

	//
	// START ETCD
	//
	// Create a temporary directory for etcd data
	tempDir, err := os.MkdirTemp("", "etcd-data-*")
	if err != nil {
		log.Fatalf("Failed to create temp dir for etcd: %v", err)
	}
	// Schedule cleanup for the end of the test run
	defer func(path string) {
		err := os.RemoveAll(path)
		if err != nil {
			log.Printf("Failed to remove temp dir for etcd: %v", err)
		}
	}(tempDir)

	// 1. Setup: Start an embedded etcd server
	cfg := embed.NewConfig()
	cfg.Dir = tempDir                      // Use the temporary directory
	cfg.LogOutputs = []string{"/dev/null"} // Silence etcd output
	etcd, err := embed.StartEtcd(cfg)
	if err != nil {
		log.Fatalf("Failed to start embedded etcd: %v", err)
	}
	defer etcd.Close()

	select {
	case <-etcd.Server.ReadyNotify():
		// Silently continue
	case <-time.After(60 * time.Second):
		etcd.Server.Stop() // trigger a shutdown
		log.Fatal("Embedded etcd server took too long to start")
	}

	// Create a client connected to the embedded server
	etcdClient, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{etcd.Clients[0].Addr().String()},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		log.Fatalf("Failed to create etcd client: %v", err)
	}
	defer func(etcdClient *clientv3.Client) {
		err := etcdClient.Close()
		if err != nil {
			log.Printf("Failed to close etcd client: %v", err)
		}
	}(etcdClient)

	echoServer, ok := setupServerledge(outboundIp.String())
	if ok != nil {
		fmt.Printf("failed to initialize serverledgde: %v\n", ok)
		os.Exit(int(codes.Internal))
	}

	waitForServerReady()

	// run all test independently
	code := m.Run()

	// tear down containers in order
	err = teardownServerledge(echoServer)
	if err != nil {
		fmt.Printf("failed to remove serverledgde: %v\n", err)
	}
	os.Exit(code)
}

func waitForServerReady() {
	// Wait for the server to be ready by polling the /status endpoint. There was a race condition in the test
	// especially noticeable for less powerful hardware.
	for i := 0; i < 50; i++ {
		resp, err := http.Get(fmt.Sprintf("http://%s:%d/status", HOST, PORT))
		if err == nil && resp.StatusCode == http.StatusOK {
			log.Println("Server is ready.")
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	log.Fatal("Server not ready after timeout")
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
