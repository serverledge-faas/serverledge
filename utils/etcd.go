package utils

import (
	"context"
	"fmt"
	"google.golang.org/grpc/connectivity"
	"log"
	"sync"
	"time"

	"github.com/serverledge-faas/serverledge/internal/config"
	clientv3 "go.etcd.io/etcd/client/v3"
)

var etcdClient *clientv3.Client = nil
var clientMutex sync.Mutex

func GetEtcdClient() (*clientv3.Client, error) {
	clientMutex.Lock()
	defer clientMutex.Unlock()

	// reuse client
	if etcdClient != nil {
		return etcdClient, nil
	}

	log.Println("Connecting to etcd")
	etcdHost := config.GetString(config.ETCD_ADDRESS, "localhost:2379")
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{etcdHost},
		DialTimeout: 3 * time.Second,
	})
	if err != nil {
		log.Printf("Could not connect to etcd: %v", err)
		return nil, fmt.Errorf("Could not connect to etcd: %v", err)
	}

	etcdClient = cli

	startConnectionMonitor(etcdClient)

	return cli, nil
}

func TryEtcdReconnection() {
	log.Println("Trying Etcd Reconnection....")
	etcdClient = nil
	_, _ = GetEtcdClient()
}

func startConnectionMonitor(cli *clientv3.Client) {
	conn := cli.ActiveConnection()

	go func() {
		for {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			changed := conn.WaitForStateChange(ctx, conn.GetState())
			cancel()

			if !changed {
				// Timeout expired with no state change — still in same state
				continue
			}

			state := conn.GetState()
			log.Printf("etcd connection state: %v", state)

			switch state {
			case connectivity.Ready:
				log.Println("etcd: connected and ready")
			case connectivity.TransientFailure:
				log.Println("etcd: transient failure, triggering reconnect")
				conn.Connect() // Force reconnect attempt
			case connectivity.Idle:
				log.Println("etcd: idle, nudging connection")
				conn.Connect()
			case connectivity.Shutdown:
				log.Println("etcd: connection shut down, exiting monitor")
				return
			default:
			}
		}
	}()
}
