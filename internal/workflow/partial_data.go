package workflow

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/serverledge-faas/serverledge/utils"
)

// PartialData is saved separately from progressData to avoid cluttering the Progress struct and each Serverledge node's cache
type PartialData struct {
	ReqId   ReqId  // request referring to this partial data
	ForTask TaskId // task that should receive this partial data
	Data    map[string]interface{}
}

func (pd PartialData) Equals(pd2 *PartialData) bool {

	if len(pd.Data) != len(pd2.Data) {
		return false
	}

	for s := range pd.Data {
		// we convert the type to string to avoid checking all possible types!!!
		value1 := fmt.Sprintf("%v", pd.Data[s])
		value2 := fmt.Sprintf("%v", pd2.Data[s])
		if value1 != value2 {
			return false
		}
	}

	return pd.ReqId == pd2.ReqId && pd.ForTask == pd2.ForTask
}

func (pd PartialData) String() string {
	return fmt.Sprintf(`PartialData{
		Id:    %s,
		ForTask:  %s,
		Data:     %v,
	}`, pd.ReqId, pd.ForTask, pd.Data)
}

func NewPartialData(reqId ReqId, forTask TaskId, data map[string]interface{}) *PartialData {
	return &PartialData{
		ReqId:   reqId,
		ForTask: forTask,
		Data:    data,
	}
}

func getPartialDataEtcdKey(reqId ReqId, nodeId TaskId) string {
	return fmt.Sprintf("/partialData/%s/%s", reqId, nodeId)
}

func SavePartialData(pd *PartialData) error {
	cli, err := utils.GetEtcdClient()
	if err != nil {
		return err
	}
	ctx := context.TODO()
	// marshal the progress object into json
	payload, err := json.Marshal(pd)
	if err != nil {
		return fmt.Errorf("could not marshal progress: %v", err)
	}
	// saves the json object into etcd
	key := getPartialDataEtcdKey(pd.ReqId, pd.ForTask)
	log.Printf("Saving PD on etcd with key: %s\n", key)

	_, err = cli.Put(ctx, key, string(payload))
	if err != nil {
		return fmt.Errorf("failed etcd Put partial data: %v", err)
	}
	return nil
}

func RetrievePartialData(reqId ReqId, nodeId TaskId) ([]*PartialData, error) {
	cli, err := utils.GetEtcdClient()
	if err != nil {
		return nil, errors.New("failed to connect to ETCD")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	key := getPartialDataEtcdKey(reqId, nodeId)
	getResponse, err := cli.Get(ctx, key)
	if err != nil || len(getResponse.Kvs) < 1 {
		return nil, fmt.Errorf("failed to retrieve partialDatas for requestId: %s", key)
	}
	partialDatas := make([]*PartialData, 0, len(getResponse.Kvs))
	for _, v := range getResponse.Kvs {
		var partialData *PartialData
		err = json.Unmarshal(v.Value, &partialData)
		if err != nil {
			return nil, fmt.Errorf("failed to unmarshal partialDatas json: %v", err)
		}
		partialDatas = append(partialDatas, partialData)
	}

	if len(partialDatas) == 0 {
		return nil, fmt.Errorf("partial data are empty")
	}

	return partialDatas, nil
}
