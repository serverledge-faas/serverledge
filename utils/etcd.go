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
var startedConnMonitor bool
var etcdReconnectionTrigger chan struct{} = nil

func GetEtcdClient() (*clientv3.Client, error) {
	clientMutex.Lock()
	defer clientMutex.Unlock()

	if etcdReconnectionTrigger == nil {
		etcdReconnectionTrigger = make(chan struct{})
	}

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

	log.Println("Connected to etcd")

	etcdClient = cli

	if !startedConnMonitor {
		startConnectionMonitor()
		startedConnMonitor = true
	}

	return cli, nil
}

func TriggerEtcdReconnection() {
	log.Println("Trying Etcd Reconnection....")
	select {
	case etcdReconnectionTrigger <- struct{}{}:
	default:
	}
}

func startConnectionMonitor() {
	go func() {
		for {
			conn := etcdClient.ActiveConnection()
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)

			// Wait for either a state change or a manual trigger
			stateCh := make(chan bool, 1)
			go func() {
				stateCh <- conn.WaitForStateChange(ctx, conn.GetState())
			}()

			var changed bool
			select {
			case changed = <-stateCh:
				// Either timed out or state changed naturally
			case <-etcdReconnectionTrigger:
				changed = true // Treat manual trigger as a state change
			}
			cancel()

			if !changed {
				continue
			}

			state := conn.GetState()
			log.Printf("etcd connection state: %v", state)

			switch state {
			case connectivity.Ready:
				log.Println("etcd: connected and ready")
			case connectivity.TransientFailure:
				log.Println("etcd: transient failure, triggering reconnect")
				conn.Connect()
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
