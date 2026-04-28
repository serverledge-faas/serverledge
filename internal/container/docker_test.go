package container

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/exec"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"

	"github.com/stretchr/testify/assert"
)

var etcdClient *clientv3.Client

// TestMain will be executed before all tests in this package.
// It is used to set up a global state, like an embedded etcd server.
func TestMain(m *testing.M) {
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
	etcdClient, err = clientv3.New(clientv3.Config{
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

	InitDockerContainerFactory()

	// 2. Run all tests
	code := m.Run()

	// 3. Teardown: The deferred calls to Close() and RemoveAll() will handle this.
	os.Exit(code)
}

// TestGetImageArchitectures requires a running Docker daemon.
func TestGetImageArchitectures(t *testing.T) {
	// Pull a known multi-arch image for testing
	_ = pullImage("busybox:latest") // So we can test both with images available locally and later we'll use
	// images not yet pulled, to test different scenarios

	t.Run("multi-arch image", func(t *testing.T) {
		archs, err := cf.GetImageArchitectures("busybox:latest")
		assert.NoError(t, err)
		assert.Contains(t, archs, "amd64")
		assert.Contains(t, archs, "arm64")
		assert.Equal(t, len(archs), 2, "Busybox should have exactly 2 architectures")
	})

	t.Run("single-arch image", func(t *testing.T) {
		image := "amd64/hello-world"
		//Let's make sure is not available locally
		cmd := exec.Command("docker", "image", "rm", image)
		_ = cmd.Run() // error here means it was already non-available locally, so it's ok

		archs, err := cf.GetImageArchitectures(image)
		assert.NoError(t, err)
		assert.Equal(t, []string{"amd64"}, archs)
		assert.Equal(t, len(archs), 1, "amd64/hello-world should have exactly 1 architecture")
	})

	t.Run("non-existent image", func(t *testing.T) {
		_, err := cf.GetImageArchitectures("non-existent-image-serverledge-test:latest")
		assert.Error(t, err)
	})
}

func TestCache(t *testing.T) {
	image := "memcached:latest"

	// This is the key that the GetImageArchitectures function will use in etcd.
	// Make sure it matches the implementation in docker.go
	cacheKey := fmt.Sprintf("/serverledge/image_architectures/%s", image)

	// 1. First call: Cache Miss
	// Before the call, the key must not exist in etcd.
	resp, err := etcdClient.Get(context.Background(), cacheKey)
	assert.NoError(t, err)
	assert.Zero(t, resp.Count, "Key should not exist before first call")

	start := time.Now()
	archs1, err := cf.GetImageArchitectures(image)
	noCacheTime := time.Since(start)
	assert.NoError(t, err)
	assert.NotEmpty(t, archs1)

	// After the call, the key must exist.
	resp, err = etcdClient.Get(context.Background(), cacheKey)
	assert.NoError(t, err)
	assert.Equal(t, int64(1), resp.Count, "Key should exist after first call")

	// 2. Second call: Cache Hit
	// For this test, we rely on the fact that the key exists and the function returns the correct value.
	start = time.Now()
	archs2, err := cf.GetImageArchitectures(image)
	cacheTime := time.Since(start)
	assert.NoError(t, err)
	assert.Equal(t, archs1, archs2, "Architectures from cache should match original ones")
	log.Printf("noCacheTime: %v, cacheTime: %v", noCacheTime, cacheTime)
}

// pullImage is a helper function to ensure the image is available locally.
func pullImage(image string) error {
	cmd := exec.Command("docker", "pull", image)

	// Run esegue il comando e attende la fine.
	err := cmd.Run()
	if err != nil {
		log.Printf("Failed pulling image: %v\n", err)
		return err
	}
	return nil

}
