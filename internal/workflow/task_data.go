package workflow

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	clientv3 "go.etcd.io/etcd/client/v3"
	"log"
	"time"

	"github.com/serverledge-faas/serverledge/utils"
)

// TaskData stores input/output data of a Task
type TaskData struct {
	Data map[string]interface{}
}

func (td TaskData) Equals(other *TaskData) bool {

	if len(td.Data) != len(other.Data) {
		return false
	}

	for s := range td.Data {
		// we convert the type to string to avoid checking all possible types!!!
		value1 := fmt.Sprintf("%v", td.Data[s])
		value2 := fmt.Sprintf("%v", other.Data[s])
		if value1 != value2 {
			return false
		}
	}

	return true
}

func (td TaskData) String() string {
	return fmt.Sprintf(`TaskData{
		Data:     %v,
	}`, td.Data)
}

func NewTaskData(data map[string]interface{}) *TaskData {
	return &TaskData{
		Data: data,
	}
}

func getTaskDataEtcdDir(reqId ReqId) string {
	return fmt.Sprintf("/data/%s", reqId)
}

func getTaskDataEtcdKey(reqId ReqId, task TaskId) string {
	return fmt.Sprintf("%s/%s", getTaskDataEtcdDir(reqId), task)
}

func (td *TaskData) Save(reqId ReqId, task TaskId) error {
	cli, err := utils.GetEtcdClient()
	if err != nil {
		return err
	}
	ctx := context.TODO()
	// marshal the progress object into json
	payload, err := json.Marshal(td)
	if err != nil {
		return fmt.Errorf("could not marshal progress: %v", err)
	}
	// saves the json object into etcd
	key := getTaskDataEtcdKey(reqId, task)
	log.Printf("Saving PD on etcd with key: %s\n", key)

	_, err = cli.Put(ctx, key, string(payload))
	if err != nil {
		return fmt.Errorf("failed etcd Put partial data: %v", err)
	}
	return nil
}

func RetrievePartialData(reqId ReqId, task TaskId) (*TaskData, error) {
	cli, err := utils.GetEtcdClient()
	if err != nil {
		return nil, errors.New("failed to connect to ETCD")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	key := getTaskDataEtcdKey(reqId, task)
	getResponse, err := cli.Get(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve partialDatas for requestId: %s", key)
	}
	if len(getResponse.Kvs) > 1 {
		return nil, fmt.Errorf("more than 1 TaskData associated with key: %s", key)
	}
	var partialData *TaskData
	for _, v := range getResponse.Kvs {
		err = json.Unmarshal(v.Value, &partialData)
		if err != nil {
			return nil, fmt.Errorf("failed to unmarshal partialDatas json: %v", err)
		}
		return partialData, nil
	}

	return nil, fmt.Errorf("failed to retrieve partialDatas for requestId: %s", key)
}
func DeleteAllTaskData(reqId ReqId) error {
	cli, err := utils.GetEtcdClient()
	if err != nil {
		return errors.New("failed to connect to ETCD")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	key := getTaskDataEtcdDir(reqId)
	_, err = cli.Delete(ctx, key, clientv3.WithPrefix())
	if err != nil {
		return fmt.Errorf("failed to delete task data for requestId: %s", key)
	}
	return nil
}
