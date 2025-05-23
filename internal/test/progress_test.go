package test

import (
	"encoding/json"
	"fmt"
	"github.com/serverledge-faas/serverledge/internal/workflow"
	u "github.com/serverledge-faas/serverledge/utils"
	"testing"
	"time"
)

func simpleProgress(t *testing.T) (*workflow.Progress, *workflow.Workflow) {
	py, err := initializeExamplePyFunction()
	u.AssertNil(t, err)
	wflow, err := CreateSequenceWorkflow(py, py)
	u.AssertNil(t, err)
	return workflow.InitProgress("simple", wflow), wflow
}

func choiceProgress(t *testing.T, condition workflow.Condition) (*workflow.Progress, *workflow.Workflow) {
	py, err := initializeExamplePyFunction()
	u.AssertNil(t, err)

	notCondition := workflow.NewPredicate().Not(condition).Build()

	wflow, err := workflow.NewBuilder().
		AddChoiceNode(
			notCondition,
			condition,
		).
		NextBranch(CreateSequenceWorkflow(py)).
		NextBranch(CreateSequenceWorkflow(py, py)).
		EndChoiceAndBuild()
	u.AssertNil(t, err)

	return workflow.InitProgress("abc", wflow), wflow
}

func TestProgressMarshaling(t *testing.T) {
	condition := workflow.NewPredicate().And(
		workflow.NewEqCondition(1, 3),
		workflow.NewGreaterCondition(1, 3),
	).Build()

	progress1, _ := simpleProgress(t)
	progress2, _ := choiceProgress(t, condition)
	progresses := []*workflow.Progress{progress1, progress2}

	for i, progress := range progresses {
		marshal, errMarshal := json.Marshal(progress)
		u.AssertNilMsg(t, errMarshal, "error during marshaling "+string(rune(i)))
		var retrieved workflow.Progress
		errUnmarshal := json.Unmarshal(marshal, &retrieved)
		u.AssertNilMsg(t, errUnmarshal, "failed composition unmarshal "+string(rune(i)))
		u.AssertTrueMsg(t, retrieved.Equals(progress), fmt.Sprintf("retrieved progress is not equal to initial progress. Retrieved:\n%s,\nExpected:\n%s ", retrieved.String(), progress.String()))
	}
}

func TestProgressCache(t *testing.T) {
	// it's an integration test because it needs etcd
	if testing.Short() {
		t.Skip()
	}

	condition := workflow.NewPredicate().And(
		workflow.NewEqCondition(1, 3),
		workflow.NewGreaterCondition(1, 3),
	).Build()

	progress1, workflow1 := simpleProgress(t)
	progress2, workflow2 := choiceProgress(t, condition)
	progresses := []*workflow.Progress{progress1, progress2}
	workflows := []*workflow.Workflow{workflow1, workflow2}

	for i := 0; i < len(workflows); i++ {
		progress := progresses[i]
		wflow := workflows[i]
		err := workflow.SaveProgress(progress)
		u.AssertNilMsg(t, err, "failed to save progress")

		retrievedProgress, err := workflow.RetrieveProgress(progress.ReqId)
		u.AssertNilMsg(t, err, "progress not found")
		u.AssertTrueMsg(t, progress.Equals(retrievedProgress), "progresses don't match")

		progress.Complete(wflow.Start.Id)
		progress.Complete(wflow.Start.GetNext())

		err = workflow.SaveProgress(progress)
		u.AssertNilMsg(t, err, "failed to save after update")

		retrievedProgress, err = workflow.RetrieveProgress(progress.ReqId)
		u.AssertNilMsg(t, err, "progress not found after update")
		u.AssertTrueMsg(t, progress.Equals(retrievedProgress), "progresses don't match after update")

		err = workflow.DeleteProgress(progress.ReqId)
		u.AssertNilMsg(t, err, "failed to delete progress")

		time.Sleep(200 * time.Millisecond)

		_, err = workflow.RetrieveProgress(progress.ReqId)
		u.AssertNonNilMsg(t, err, "progress not found after delete")
	}
}
