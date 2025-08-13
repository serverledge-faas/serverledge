package utils

import (
	"fmt"
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

	log.Println("Connected to etcd")

	etcdClient = cli
	return cli, nil
}
