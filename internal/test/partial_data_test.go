package test

import (
	"encoding/json"
	"fmt"
	"github.com/serverledge-faas/serverledge/internal/cache"
	"github.com/serverledge-faas/serverledge/internal/workflow"
	u "github.com/serverledge-faas/serverledge/utils"
	"testing"
	"time"
)

func TestPartialDataMarshaling(t *testing.T) {
	data := make(map[string]interface{})
	data["prova"] = "testo"
	data["num"] = 2
	data["list"] = []string{"uno", "due", "tre"}
	partialData := workflow.PartialData{
		ReqId:    workflow.ReqId("abc"),
		ForTask:  "fai13p102",
		FromTask: "120e8d12d",
		Data:     data,
	}
	marshal, errMarshal := json.Marshal(partialData)
	u.AssertNilMsg(t, errMarshal, "error during marshaling")
	var retrieved workflow.PartialData
	errUnmarshal := json.Unmarshal(marshal, &retrieved)
	u.AssertNilMsg(t, errUnmarshal, "failed composition unmarshal")

	u.AssertTrueMsg(t, retrieved.Equals(&partialData), fmt.Sprintf("retrieved partialData is not equal to initial partialData. Retrieved:\n%s,\nExpected:\n%s ", retrieved.String(), partialData.String()))
}

func TestPartialDataCache(t *testing.T) {
	// it's an integration test because it needs etcd
	if testing.Short() {
		t.Skip()
	}

	request1 := workflow.ReqId("abc")
	request2 := workflow.ReqId("zzz")

	data := make(map[string]interface{})
	data["num"] = 1
	partialData1 := initPartialData(request1, "nodo1", "start", data)
	data = make(map[string]interface{})
	data["num"] = 2
	partialData2 := initPartialData(request1, "nodo2", "nodo1", data)
	data = make(map[string]interface{})
	data["num"] = 3
	partialData3 := initPartialData(request2, "start", "", data)
	partialDatas := []*workflow.PartialData{partialData1, partialData2, partialData3}

	// saving and retrieving partial datas one by one
	for i := 0; i < len(partialDatas); i++ {
		partialData := partialDatas[i]
		err := workflow.SavePartialData(partialData, cache.Persist)
		u.AssertNilMsg(t, err, "failed to save partialData")

		retrievedPartialData, err := workflow.RetrievePartialData(partialData.ReqId, partialData.ForTask, cache.Persist)
		u.AssertNilMsg(t, err, "partialData not found")
		u.AssertTrueMsg(t, partialData.Equals(retrievedPartialData[0]), "progresses don't match")

		_, err = workflow.DeleteAllPartialData(partialData.ReqId, cache.Persist)
		u.AssertNilMsg(t, err, "failed to delete partialData")

		_, err = workflow.RetrievePartialData(partialData.ReqId, partialData.ForTask, cache.Persist)
		u.AssertNonNilMsg(t, err, "partialData should have been deleted")
	}

	requests := []workflow.ReqId{request1, request2}
	partialDataMap := make(map[workflow.ReqId][]*workflow.PartialData)
	partialDataMap[request1] = make([]*workflow.PartialData, 0, 2)
	partialDataMap[request1] = append(partialDataMap[request1], partialData1, partialData2)
	partialDataMap[request2] = make([]*workflow.PartialData, 0, 1)
	partialDataMap[request2] = append(partialDataMap[request2], partialData3)

	// saving, retrieving and deleting partial data request by request
	for i := 0; i < len(requests); i++ {
		request := requests[i]
		partialDataList := partialDataMap[request]
		for _, partialData := range partialDataList {
			err := workflow.SavePartialData(partialData, cache.Persist)
			u.AssertNilMsg(t, err, "failed to save partialData")
		}

		retrievedPartialData, err := workflow.RetrieveAllPartialData(request, cache.Persist)
		u.AssertNil(t, err)
		count := 0
		retrievedPartialData.Range(func(key, value any) bool {
			count++
			return true
		})
		u.AssertEqualsMsg(t, len(partialDataList), count, "number of partial data for request  differs")

		_, err = workflow.DeleteAllPartialData(request, cache.Persist)
		u.AssertNilMsg(t, err, "failed to delete all partialData")

		time.Sleep(200 * time.Millisecond)

		numPartialData := workflow.NumberOfPartialDataFor(request, cache.Persist)
		u.AssertEqualsMsg(t, 0, numPartialData, "retrieved partialData should have been 0")
	}
}

func initPartialData(reqId workflow.ReqId, to, from workflow.TaskId, data map[string]interface{}) *workflow.PartialData {
	return &workflow.PartialData{
		ReqId:    reqId,
		ForTask:  to,
		FromTask: from,
		Data:     data,
	}
}
