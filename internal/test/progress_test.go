package test

import (
	"encoding/json"
	"fmt"
	"github.com/grussorusso/serverledge/internal/cache"
	"github.com/grussorusso/serverledge/internal/workflow"
	u "github.com/grussorusso/serverledge/utils"
	"testing"
	"time"
)

func simpleProgress(t *testing.T) (*workflow.Progress, *workflow.Workflow) {
	py, err := initializeExamplePyFunction()
	u.AssertNil(t, err)
	wflow, err := workflow.CreateSequenceWorkflow(py, py)
	u.AssertNil(t, err)
	return workflow.InitProgressRecursive("simple", wflow), wflow
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
		NextBranch(workflow.CreateSequenceWorkflow(py)).
		NextBranch(workflow.CreateSequenceWorkflow(py, py)).
		EndChoiceAndBuild()
	u.AssertNil(t, err)

	return workflow.InitProgressRecursive("abc", wflow), wflow
}

func parallelProgress(t *testing.T) (*workflow.Progress, *workflow.Workflow) {
	py, err := initializeExamplePyFunction()
	u.AssertNil(t, err)

	wflow, err := workflow.NewBuilder().
		AddBroadcastFanOutNode(3).
		NextFanOutBranch(workflow.CreateSequenceWorkflow(py)).
		NextFanOutBranch(workflow.CreateSequenceWorkflow(py, py)).
		NextFanOutBranch(workflow.CreateSequenceWorkflow(py, py, py)).
		AddFanInNode(workflow.AddNewMapEntry).
		Build()
	u.AssertNil(t, err)

	return workflow.InitProgressRecursive("abc", wflow), wflow
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
		NextBranch(workflow.CreateSequenceWorkflow(py)).
		NextBranch(workflow.NewBuilder().
			AddBroadcastFanOutNode(3).
			ForEachParallelBranch(func() (*workflow.Workflow, error) { return workflow.CreateSequenceWorkflow(py, py) }).
			AddFanInNode(workflow.AddNewMapEntry).
			Build()).
		EndChoiceAndBuild()
	u.AssertNil(t, err)

	return workflow.InitProgressRecursive("abc", wflow), wflow
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

		err = progress.CompleteNode(wflow.Start.Id)
		u.AssertNilMsg(t, err, "failed to update progress")
		err = progress.CompleteNode(wflow.Start.Next)
		u.AssertNilMsg(t, err, "failed to update progress")

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

// TestProgressSequence tests a sequence workflow with 2 simple node
func TestProgressSequence(t *testing.T) {
	progress, workflow := simpleProgress(t)

	// Start node
	checkAndCompleteNext(t, progress, workflow)

	// Simple Node 1
	checkAndCompleteNext(t, progress, workflow)

	// Simple Node 2
	checkAndCompleteNext(t, progress, workflow)

	// End node
	checkAndCompleteNext(t, progress, workflow)

	// End of workflow
	finishProgress(t, progress)
}

// TestProgressChoice1 tests the left branch
func TestProgressChoice1(t *testing.T) {
	condition := workflow.NewPredicate().And(
		workflow.NewEqCondition(1, 3),
		workflow.NewGreaterCondition(1, 3),
	).Build()
	progress, wflow := choiceProgress(t, condition)

	// Start node
	checkAndCompleteNext(t, progress, wflow)

	// Choice node
	choice := checkAndCompleteNext(t, progress, wflow).(*workflow.ChoiceNode)

	// Simple node (left) // suppose the left condition is true
	checkAndCompleteChoice(t, progress, choice, wflow)

	// End
	checkAndCompleteNext(t, progress, wflow)

	// End of workflow
	finishProgress(t, progress)
}

// TestProgressChoice2 tests the right branch
func TestProgressChoice2(t *testing.T) {
	condition := workflow.NewPredicate().And(
		workflow.NewEqCondition(1, 1),
		workflow.NewGreaterCondition(5, 3),
	).Build()
	progress, wflow := choiceProgress(t, condition)

	// Start node
	checkAndCompleteNext(t, progress, wflow)

	// Choice node
	choice := checkAndCompleteNext(t, progress, wflow).(*workflow.ChoiceNode)

	// Simple Node left is skipped, right is executed
	checkAndCompleteChoice(t, progress, choice, wflow)

	// Simple Node right 2
	checkAndCompleteNext(t, progress, wflow)

	// End node
	checkAndCompleteNext(t, progress, wflow)

	// End of workflow
	finishProgress(t, progress)
}

func TestParallelProgress(t *testing.T) {
	progress, wflow := parallelProgress(t)

	// Start node
	checkAndCompleteNext(t, progress, wflow)

	// FanOut node
	checkAndCompleteNext(t, progress, wflow)

	// 3 Simple Nodes in parallel
	checkAndCompleteMultiple(t, progress, wflow)
	// simpleNode1 := fanOut.GetNext()[0]
	// simpleNode2 := fanOut.GetNext()[1]
	// simpleNode3 := fanOut.GetNext()[2]

	// 2 Simple Nodes in parallel // here should get two nodes
	checkAndCompleteMultiple(t, progress, wflow)
	// nextNode = progress.NextNodes()
	// simpleNodeCentral2 := simpleNode2.GetNext()[0]
	// u.AssertEquals(t, nextNode[0], simpleNodeCentral2.GetId())
	// u.AssertEquals(t, 3, progress.GetGroup(nextNode[0]))
	// err = progress.CompleteNode(nextNode[0])
	// u.AssertNil(t, err)
	// simpleNodeCentral3 := simpleNode3.GetNext()[0]
	// u.AssertEquals(t, nextNode[1], simpleNodeCentral3.GetId())
	// u.AssertEquals(t, 3, progress.GetGroup(nextNode[1]))
	// err = progress.CompleteNode(nextNode[1])
	// u.AssertNil(t, err)

	// 1 Simple node (parallel) right, bottom
	checkAndCompleteMultiple(t, progress, wflow)
	// nextNode = progress.NextNodes()
	// simpleNodeBottom3 := simpleNodeCentral3.GetNext()[0]
	// u.AssertEquals(t, nextNode[0], simpleNodeBottom3.GetId())
	// u.AssertEquals(t, 4, progress.GetGroup(nextNode[0]))
	// err = progress.CompleteNode(nextNode[0])
	// u.AssertNil(t, err)

	// Fan in
	checkAndCompleteNext(t, progress, wflow)

	// End node
	checkAndCompleteNext(t, progress, wflow)

	// End of workflow
	finishProgress(t, progress)
}

func TestComplexProgress(t *testing.T) {
	condition := workflow.NewPredicate().And(
		workflow.NewEqCondition(1, 3),
		workflow.NewGreaterCondition(1, 3),
	).Build()
	progress, wflow := complexProgress(t, condition)

	// Start node
	checkAndCompleteNext(t, progress, wflow)

	// SimpleNode
	checkAndCompleteNext(t, progress, wflow)

	// Choice
	choice := checkAndCompleteNext(t, progress, wflow).(*workflow.ChoiceNode)

	// Simple Node, FanOut
	checkAndCompleteChoice(t, progress, choice, wflow)

	// End node
	checkAndCompleteNext(t, progress, wflow)

	// End of workflow
	finishProgress(t, progress)
}

func TestComplexProgress2(t *testing.T) {
	condition := workflow.NewPredicate().And(
		workflow.NewEqCondition(1, 1),
		workflow.NewGreaterCondition(4, 3),
	).Build()
	progress, wflow := complexProgress(t, condition)

	// Start node
	checkAndCompleteNext(t, progress, wflow)

	// Simple Node
	checkAndCompleteNext(t, progress, wflow)

	// Choice
	choice := checkAndCompleteNext(t, progress, wflow).(*workflow.ChoiceNode)

	// Simple Node, FanOut // suppose the fanout node at the right and all its children are skipped
	checkAndCompleteChoice(t, progress, choice, wflow)

	// 3 Simple Nodes in parallel
	checkAndCompleteMultiple(t, progress, wflow)

	// 3 other Simple Nodes
	checkAndCompleteMultiple(t, progress, wflow)

	// Fan in
	checkAndCompleteNext(t, progress, wflow)

	// End node
	checkAndCompleteNext(t, progress, wflow)

	// End of workflow
	finishProgress(t, progress)
}

func checkAndCompleteNext(t *testing.T, progress *workflow.Progress, workflow *workflow.Workflow) workflow.Task {
	nextNode, err := progress.NextNodes()
	u.AssertNil(t, err)
	nodeId := nextNode[0]
	node, ok := workflow.Find(nodeId)
	u.AssertTrue(t, ok)
	u.AssertEquals(t, nodeId, node.GetId())
	u.AssertEquals(t, progress.NextGroup, progress.GetGroup(nodeId))
	err = progress.CompleteNode(nodeId)
	u.AssertNil(t, err)
	return node
}

func checkAndCompleteChoice(t *testing.T, progress *workflow.Progress, choice *workflow.ChoiceNode, workflow *workflow.Workflow) {
	nextNode, err := progress.NextNodes() // Simple1, Simple2
	u.AssertNil(t, err)
	simpleNodeLeft := choice.Alternatives[0]
	fanOut := choice.Alternatives[1]
	u.AssertEquals(t, nextNode[0], simpleNodeLeft)
	u.AssertEquals(t, nextNode[1], fanOut)
	u.AssertEquals(t, progress.NextGroup, progress.GetGroup(nextNode[0]))
	u.AssertEquals(t, progress.NextGroup, progress.GetGroup(nextNode[1]))

	_, _ = choice.Exec(newCompositionRequestTest(), make(map[string]interface{}))
	err = progress.CompleteNode(nextNode[choice.FirstMatch])
	u.AssertNil(t, err)
	nodeToSkip := choice.GetNodesToSkip(workflow)
	err = progress.SkipAll(nodeToSkip)
	u.AssertNil(t, err)
}

func checkAndCompleteMultiple(t *testing.T, progress *workflow.Progress, wflow *workflow.Workflow) []workflow.Task {
	nextNode, err := progress.NextNodes()
	completedNodes := make([]workflow.Task, 0)
	u.AssertNil(t, err)
	for _, nodeId := range nextNode {
		node, ok := wflow.Find(nodeId)
		u.AssertTrue(t, ok)
		u.AssertEquals(t, nodeId, node.GetId())
		u.AssertEquals(t, progress.NextGroup, progress.GetGroup(nodeId))
		err = progress.CompleteNode(nodeId)
		u.AssertNil(t, err)
		completedNodes = append(completedNodes, node)
	}
	return completedNodes
}

func finishProgress(t *testing.T, progress *workflow.Progress) {
	nothing, err := progress.NextNodes()
	u.AssertNil(t, err)
	u.AssertEmptySlice(t, nothing)
}
