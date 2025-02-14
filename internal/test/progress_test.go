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

func parallelProgress(t *testing.T) (*workflow.Progress, *workflow.Workflow) {
	py, err := initializeExamplePyFunction()
	u.AssertNil(t, err)

	wflow, err := workflow.NewBuilder().
		AddBroadcastFanOutNode(3).
		NextFanOutBranch(CreateSequenceWorkflow(py)).
		NextFanOutBranch(CreateSequenceWorkflow(py, py)).
		NextFanOutBranch(CreateSequenceWorkflow(py, py, py)).
		AddFanInNode().
		Build()
	u.AssertNil(t, err)

	return workflow.InitProgress("abc", wflow), wflow
}

func complexProgress(t *testing.T, condition workflow.Condition) (*workflow.Progress, *workflow.Workflow) {
	py, err := initializeExamplePyFunction()
	u.AssertNil(t, err)

	notCondition := workflow.NewPredicate().Not(condition).Build()

	wflow, err := workflow.NewBuilder().
		AddSimpleNode(py).
		AddChoiceNode(
			notCondition,
			condition,
		).
		NextBranch(CreateSequenceWorkflow(py)).
		NextBranch(workflow.NewBuilder().
			AddBroadcastFanOutNode(3).
			ForEachParallelBranch(func() (*workflow.Workflow, error) { return CreateSequenceWorkflow(py, py) }).
			AddFanInNode().
			Build()).
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
	progress3, _ := parallelProgress(t)
	progress4, _ := complexProgress(t, condition)
	progresses := []*workflow.Progress{progress1, progress2, progress3, progress4}

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
	progress3, workflow3 := parallelProgress(t)
	progress4, workflow4 := complexProgress(t, condition)
	progresses := []*workflow.Progress{progress1, progress2, progress3, progress4}
	workflows := []*workflow.Workflow{workflow1, workflow2, workflow3, workflow4}

	for i := 0; i < len(workflows); i++ {
		progress := progresses[i]
		wflow := workflows[i]
		err := workflow.SaveProgress(progress, cache.Persist)
		u.AssertNilMsg(t, err, "failed to save progress")

		retrievedProgress, found := workflow.RetrieveProgress(progress.ReqId, cache.Persist)
		u.AssertTrueMsg(t, found, "progress not found")
		u.AssertTrueMsg(t, progress.Equals(retrievedProgress), "progresses don't match")

		progress.Complete(wflow.Start.Id)
		progress.Complete(wflow.Start.Next)

		err = workflow.SaveProgress(progress, cache.Persist)
		u.AssertNilMsg(t, err, "failed to save after update")

		retrievedProgress, found = workflow.RetrieveProgress(progress.ReqId, cache.Persist)
		u.AssertTrueMsg(t, found, "progress not found after update")
		u.AssertTrueMsg(t, progress.Equals(retrievedProgress), "progresses don't match after update")

		err = workflow.DeleteProgress(progress.ReqId, cache.Persist)
		u.AssertNilMsg(t, err, "failed to delete progress")

		time.Sleep(200 * time.Millisecond)

		_, found = workflow.RetrieveProgress(progress.ReqId, cache.Persist)
		u.AssertFalseMsg(t, found, "progress should have been deleted")
	}
}
