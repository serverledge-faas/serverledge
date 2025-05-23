package test

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"testing"

	"github.com/serverledge-faas/serverledge/internal/workflow"
	u "github.com/serverledge-faas/serverledge/utils"
)

func TestWorkflowMarshaling(t *testing.T) {
	f, _ := initializeExamplePyFunction()

	workflow1, _ := workflow.CreateEmptyWorkflow()
	workflow2, _ := CreateSequenceWorkflow(f, f, f)
	workflow3, _ := CreateChoiceWorkflow(func() (*workflow.Workflow, error) { return CreateSequenceWorkflow(f, f) })
	workflows := []*workflow.Workflow{workflow1, workflow2, workflow3}
	for i, wflow := range workflows {
		marshal, errMarshal := json.Marshal(wflow)
		u.AssertNilMsg(t, errMarshal, "error during marshaling "+string(rune(i)))
		var retrieved workflow.Workflow
		errUnmarshal := json.Unmarshal(marshal, &retrieved)
		u.AssertNilMsg(t, errUnmarshal, "failed composition unmarshal "+string(rune(i)))
		u.AssertTrueMsg(t, retrieved.Equals(wflow),
			fmt.Sprintf("retrieved workflow is not equal to initial workflow. Retrieved:\n%s,\nExpected:\n%s ", retrieved.String(), wflow.String()))
	}
}

// test for workflow connections
func TestEmptyWorkflow(t *testing.T) {
	// workflow.BranchNumber = 0

	input := 1
	m := make(map[string]interface{})
	m["input"] = input
	wflow, err := workflow.CreateEmptyWorkflow()
	u.AssertNil(t, err)

	u.AssertNonNil(t, wflow.Start)
	u.AssertNonNil(t, wflow.End)
	u.AssertNonNil(t, wflow.Tasks)
	u.AssertEquals(t, wflow.Start.GetNext(), wflow.End.GetId())
}

// TestSimpleWorkflow creates a simple Workflow with one StartTask, two SimpleTask and one EndTask, executes it and gets the result.
func TestSimpleWorkflow(t *testing.T) {
	//workflow.BranchNumber = 0

	input := 1
	m := make(map[string]interface{})
	m["input"] = input
	length := 2

	f, fArr, err := initializeSameFunctionSlice(length, "js")
	u.AssertNil(t, err)

	wflow, err := CreateSequenceWorkflow(fArr...)
	u.AssertNil(t, err)

	u.AssertNonNil(t, wflow.Start)
	u.AssertNonNil(t, wflow.End)
	u.AssertNonNil(t, wflow.Tasks)
	u.AssertEquals(t, len(wflow.Tasks)-2, length)

	// tasks := workflow.NewNodeSetFrom(workflow.Tasks)
	_, found := wflow.Find(wflow.Start.GetNext())
	u.AssertTrue(t, found)
	end := false
	var prevNode workflow.Task = wflow.Start
	var currentNode workflow.Task
	for !end {
		switch prevNode.(type) {
		case *workflow.StartTask:
			nextNodeId := prevNode.GetNext()
			currentNode, _ = wflow.Find(nextNodeId)
			u.AssertEquals(t, prevNode.GetNext(), currentNode.GetId())
		case *workflow.EndTask:
			end = true
		default: // currentNode = simple node
			nextNodeId := prevNode.GetNext()
			currentNode, _ = wflow.Find(nextNodeId)
			u.AssertEquals(t, prevNode.GetNext(), currentNode.GetId())
			u.AssertTrue(t, prevNode.(*workflow.SimpleTask).Func == f.Name)
		}
		prevNode = currentNode
	}
	u.AssertEquals(t, prevNode.(*workflow.EndTask), wflow.End)

	for tid, prevs := range wflow.GetAllPreviousTasks() {
		fmt.Printf("Previous tasks of %s (%s):\n", tid, wflow.Tasks[tid].GetType())
		for _, prev := range prevs {
			fmt.Printf("\t%s\n", wflow.Tasks[prev].String())
		}
	}
}

func TestChoiceWorkflow(t *testing.T) {
	//workflow.BranchNumber = 0

	m := make(map[string]interface{})
	m["input"] = 1

	arr := make([]workflow.Condition, 3)
	arr[0] = workflow.NewConstCondition(false)
	arr[1] = workflow.NewConstCondition(rand.Int()%2 == 0)
	arr[2] = workflow.NewConstCondition(true)
	f, fArr, err := initializeSameFunctionSlice(1, "js")
	u.AssertNil(t, err)

	wflow, err := CreateChoiceWorkflow(func() (*workflow.Workflow, error) { return CreateSequenceWorkflow(fArr...) }, arr...)
	u.AssertNil(t, err)

	u.AssertNonNil(t, wflow.Start)
	u.AssertNonNil(t, wflow.End)
	u.AssertNonNil(t, wflow.Tasks)
	// u.AssertEquals(t, width+1, len(workflow.Tasks))

	//tasks := workflow.NewNodeSetFrom(workflow.Tasks)
	choiceWorkflow, found := wflow.Find(wflow.Start.GetNext())
	choice := choiceWorkflow.(*workflow.ChoiceTask)
	u.AssertTrue(t, found)
	for _, n := range wflow.Tasks {
		switch n.(type) {
		case *workflow.ChoiceTask:
			u.AssertEquals(t, len(choice.Conditions), len(choice.AlternativeNextTasks))
			for _, s := range choice.AlternativeNextTasks {
				simple, foundS := wflow.Find(s)
				u.AssertTrue(t, foundS)
				u.AssertEquals(t, simple.GetNext(), wflow.End.GetId())
			}
		case *workflow.SimpleTask:
			u.AssertTrue(t, n.(*workflow.SimpleTask).Func == f.Name)
		}
	}
}

func TestChoiceWorkflow_BuiltWithNextBranch(t *testing.T) {
	//workflow.BranchNumber = 0

	m := make(map[string]interface{})
	m["input"] = 1
	length := 2
	f, fArr, err := initializeSameFunctionSlice(length, "py")
	u.AssertNil(t, err)

	wflow, err := workflow.NewBuilder().
		AddChoiceNode(
			workflow.NewConstCondition(false),
			workflow.NewSmallerCondition(2, 1),
			workflow.NewConstCondition(true),
		).
		NextBranch(CreateSequenceWorkflow(fArr...)).
		NextBranch(CreateSequenceWorkflow(fArr...)).
		NextBranch(CreateSequenceWorkflow(fArr...)).
		EndChoiceAndBuild()

	if err != nil {
		fmt.Println(err)
		t.FailNow()
	}

	choiceWorkflow, foundStartNext := wflow.Find(wflow.Start.GetNext())
	choice := choiceWorkflow.(*workflow.ChoiceTask)

	u.AssertNil(t, err)

	u.AssertNonNil(t, wflow.Start)
	u.AssertNonNil(t, wflow.End)
	u.AssertNonNil(t, wflow.Tasks)

	u.AssertTrue(t, foundStartNext)
	for _, n := range wflow.Tasks {
		switch node := n.(type) {
		case *workflow.ChoiceTask:
			u.AssertEquals(t, len(choice.Conditions), len(choice.AlternativeNextTasks))
			for _, s := range choice.AlternativeNextTasks {
				_, foundS := wflow.Find(s)
				u.AssertTrue(t, foundS)
			}
		case *workflow.SimpleTask:
			u.AssertTrue(t, node.Func == f.Name)
		}
	}
}

// TestWorkflowBuilder tests a complex Workflow with every type of node in it
//
//		    [Start ]
//	           |
//	        [Simple]
//	 	       |
//		[====Choice====] // 1 == 4, 1 != 4
//	       |        |
//	    [Simple] [FanOut] // scatter
//	       |       |3|
//	       |     [Simple]
//	       |       |3|
//	       |     [FanIn ] // AddToArrayEntry
//	       |        |
//	       |---->[ End  ]
func TestWorkflowBuilder(t *testing.T) {
	f, err := initializeExamplePyFunction()
	u.AssertNil(t, err)
	wflow, err := workflow.NewBuilder().
		AddSimpleNode(f).
		AddChoiceNode(workflow.NewEqCondition(1, 4), workflow.NewDiffCondition(1, 4)).
		NextBranch(CreateSequenceWorkflow(f)).
		NextBranch(CreateSequenceWorkflow(f)).
		EndChoiceAndBuild()

	u.AssertNil(t, err)
	// tasks := workflow.NewNodeSetFrom(workflow.Tasks)
	for _, n := range wflow.Tasks {
		switch node := n.(type) {
		case *workflow.SimpleTask:
			u.AssertTrue(t, node.Func == f.Name)
		case *workflow.ChoiceTask:
			choice := node
			u.AssertEquals(t, len(choice.Conditions), len(choice.AlternativeNextTasks))

			// specific for this test
			alt0, foundAlt0 := wflow.Find(choice.AlternativeNextTasks[0])
			alt1, foundAlt1 := wflow.Find(choice.AlternativeNextTasks[1])
			firstAlternative := alt0.(*workflow.SimpleTask)
			secondAlternative := alt1.(*workflow.SimpleTask)

			u.AssertTrue(t, foundAlt0)
			u.AssertTrue(t, foundAlt1)
			u.AssertEquals(t, firstAlternative.NextTask, wflow.End.GetId())
			u.AssertEquals(t, secondAlternative.NextTask, wflow.End.GetId())

		default:
			continue
		}
	}
}

func TestVisit(t *testing.T) {
	f, err := initializeExamplePyFunction()
	u.AssertNil(t, err)
	complexWorkflow, err := workflow.NewBuilder().
		AddSimpleNode(f).
		AddChoiceNode(workflow.NewEqCondition(1, 4), workflow.NewDiffCondition(1, 4)).
		NextBranch(CreateSequenceWorkflow(f)).
		NextBranch(CreateSequenceWorkflow(f, f)).
		EndChoiceAndBuild()
	u.AssertNil(t, err)

	startNext, _ := complexWorkflow.Find(complexWorkflow.Start.GetNext())

	choice := startNext.GetNext()

	visitedNodes := workflow.Visit(complexWorkflow, complexWorkflow.Start.Id, false)
	u.AssertEquals(t, len(complexWorkflow.Tasks), len(visitedNodes))

	visitedNodes = workflow.Visit(complexWorkflow, complexWorkflow.Start.Id, true)
	u.AssertEquals(t, len(complexWorkflow.Tasks)-1, len(visitedNodes))

	visitedNodes = workflow.Visit(complexWorkflow, choice, false)
	u.AssertEquals(t, 5, len(visitedNodes))

	visitedNodes = workflow.Visit(complexWorkflow, choice, true)
	u.AssertEquals(t, 4, len(visitedNodes))

}
