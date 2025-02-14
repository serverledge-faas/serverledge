package scheduling

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/serverledge-faas/serverledge/internal/function"
	"github.com/serverledge-faas/serverledge/utils"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func publishAsyncResponse(reqId string, response function.Response) {
	etcdClient, err := utils.GetEtcdClient()
	if err != nil {
		log.Fatal("Client not available")
		return
	}

	ctx := context.Background()

	resp, err := etcdClient.Grant(ctx, 1800) // 30 min
	if err != nil {
		log.Fatal(err)
		return
	}

	key := fmt.Sprintf("async/%s", reqId)
	payload, err := json.Marshal(response)
	if err != nil {
		log.Printf("Could not marshal response: %v\n", err)
		return
	}

	_, err = etcdClient.Put(ctx, key, string(payload), clientv3.WithLease(resp.ID))
	if err != nil {
		log.Fatal(err)
		return
	}
}
