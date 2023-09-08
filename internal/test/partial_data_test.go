package test

import (
	"encoding/json"
	"fmt"
	"github.com/grussorusso/serverledge/internal/cache"
	"github.com/grussorusso/serverledge/internal/fc"
	u "github.com/grussorusso/serverledge/utils"
	"testing"
	"time"
)

func TestPartialDataMarshaling(t *testing.T) {
	data := make(map[string]interface{})
	data["prova"] = "testo"
	data["num"] = 2
	data["list"] = []string{"uno", "due", "tre"}
	partialData := fc.PartialData{
		ReqId:    fc.ReqId("abc"),
		ForNode:  "fai13p102",
		FromNode: "120e8d12d",
		Data:     data,
	}
	marshal, errMarshal := json.Marshal(partialData)
	u.AssertNilMsg(t, errMarshal, "error during marshaling")
	var retrieved fc.PartialData
	errUnmarshal := json.Unmarshal(marshal, &retrieved)
	u.AssertNilMsg(t, errUnmarshal, "failed composition unmarshal")

	u.AssertTrueMsg(t, retrieved.Equals(&partialData), fmt.Sprintf("retrieved partialData is not equal to initial partialData. Retrieved:\n%s,\nExpected:\n%s ", retrieved.String(), partialData.String()))
}

func TestPartialDataCache(t *testing.T) {
	// it's an integration test because it needs etcd
	if !IntegrationTest {
		t.Skip()
	}

	request1 := fc.ReqId("abc")
	request2 := fc.ReqId("zzz")

	data := make(map[string]interface{})
	data["num"] = 1
	partialData1 := initPartialData(request1, "nodo1", "start", data)
	data = make(map[string]interface{})
	data["num"] = 2
	partialData2 := initPartialData(request1, "nodo2", "nodo1", data)
	data = make(map[string]interface{})
	data["num"] = 3
	partialData3 := initPartialData(request2, "start", "", data)
	partialDatas := []*fc.PartialData{partialData1, partialData2, partialData3}

	// saving and retrieving partial datas one by one
	for i := 0; i < len(partialDatas); i++ {
		partialData := partialDatas[i]
		err := fc.SavePartialData(partialData, cache.Persist)
		u.AssertNilMsg(t, err, "failed to save partialData")

		retrievedPartialData, err := fc.RetrievePartialData(partialData.ReqId, partialData.ForNode, cache.Persist)
		u.AssertNilMsg(t, err, "partialData not found")
		u.AssertTrueMsg(t, partialData.Equals(retrievedPartialData[0]), "progresses don't match")

		_, err = fc.DeleteAllPartialData(partialData.ReqId, cache.Persist)
		u.AssertNilMsg(t, err, "failed to delete partialData")

		_, err = fc.RetrievePartialData(partialData.ReqId, partialData.ForNode, cache.Persist)
		u.AssertNonNilMsg(t, err, "partialData should have been deleted")
	}

	requests := []fc.ReqId{request1, request2}
	partialDataMap := make(map[fc.ReqId][]*fc.PartialData)
	partialDataMap[request1] = make([]*fc.PartialData, 0, 2)
	partialDataMap[request1] = append(partialDataMap[request1], partialData1, partialData2)
	partialDataMap[request2] = make([]*fc.PartialData, 0, 1)
	partialDataMap[request2] = append(partialDataMap[request2], partialData3)

	// saving, retrieving and deleting partial data request by request
	for i := 0; i < len(requests); i++ {
		request := requests[i]
		partialDataList := partialDataMap[request]
		for _, partialData := range partialDataList {
			err := fc.SavePartialData(partialData, cache.Persist)
			u.AssertNilMsg(t, err, "failed to save partialData")
		}

		retrievedPartialData, err := fc.RetrieveAllPartialData(request, cache.Persist)
		u.AssertNil(t, err)
		count := 0
		retrievedPartialData.Range(func(key, value any) bool {
			count++
			return true
		})
		u.AssertEqualsMsg(t, len(partialDataList), count, "number of partial data for request  differs")

		_, err = fc.DeleteAllPartialData(request, cache.Persist)
		u.AssertNilMsg(t, err, "failed to delete all partialData")

		time.Sleep(200 * time.Millisecond)

		numPartialData := fc.NumberOfPartialDataFor(request, cache.Persist)
		u.AssertEqualsMsg(t, 0, numPartialData, "retrieved partialData should have been 0")
	}
}

func initPartialData(reqId fc.ReqId, to, from fc.DagNodeId, data map[string]interface{}) *fc.PartialData {
	return &fc.PartialData{
		ReqId:    reqId,
		ForNode:  to,
		FromNode: from,
		Data:     data,
	}
}
