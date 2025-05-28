package test

import (
	"encoding/json"
	"fmt"
	"github.com/serverledge-faas/serverledge/internal/workflow"
	u "github.com/serverledge-faas/serverledge/utils"
	"testing"
)

func TestTaskDataMarshaling(t *testing.T) {
	data := make(map[string]interface{})
	data["prova"] = "testo"
	data["num"] = 2
	data["list"] = []string{"uno", "due", "tre"}
	partialData := workflow.TaskData{
		Data: data,
	}
	marshal, errMarshal := json.Marshal(partialData)
	u.AssertNilMsg(t, errMarshal, "error during marshaling")
	var retrieved workflow.TaskData
	errUnmarshal := json.Unmarshal(marshal, &retrieved)
	u.AssertNilMsg(t, errUnmarshal, "failed composition unmarshal")

	u.AssertTrueMsg(t, retrieved.Equals(&partialData), fmt.Sprintf("retrieved partialData is not equal to initial partialData. Retrieved:\n%s,\nExpected:\n%s ", retrieved.String(), partialData.String()))
}
