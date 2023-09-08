package fc_scheduling

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/grussorusso/serverledge/internal/fc"
	"github.com/grussorusso/serverledge/utils"
	clientv3 "go.etcd.io/etcd/client/v3"
	"log"
)

func PublishAsyncCompositionResponse(reqId string, response fc.CompositionResponse) {
	etcdClient, err := utils.GetEtcdClient()
	if err != nil {
		log.Fatal("Client not available")
		return
	}

	ctx := context.Background()

	resp, err := etcdClient.Grant(ctx, 1800)
	if err != nil {
		log.Fatal(err)
		return
	}

	key := fmt.Sprintf("async/%s", reqId) // async is for function and function compositions, so we can reuse poll!!!
	payload, err := json.Marshal(response)
	if err != nil {
		log.Printf("Could not marshal response: %v", err)
		return
	}

	_, err = etcdClient.Put(ctx, key, string(payload), clientv3.WithLease(resp.ID))
	if err != nil {
		log.Fatal(err)
		return
	}
}
