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
	workflow4, _ := CreateBroadcastWorkflow(func() (*workflow.Workflow, error) { return CreateSequenceWorkflow(f, f) }, 4)
	workflow5, _ := CreateScatterSingleFunctionWorkflow(f, 5)
	workflow6, _ := CreateBroadcastMultiFunctionWorkflow(
		func() (*workflow.Workflow, error) { return CreateSequenceWorkflow(f) },
		func() (*workflow.Workflow, error) { return CreateSequenceWorkflow(f, f) },
		func() (*workflow.Workflow, error) { return CreateSequenceWorkflow(f, f, f) },
	)
	workflows := []*workflow.Workflow{workflow1, workflow2, workflow3, workflow4, workflow5, workflow6}
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
	u.AssertEquals(t, wflow.Start.Next, wflow.End.GetId())
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
	_, found := wflow.Find(wflow.Start.Next)
	u.AssertTrue(t, found)
	end := false
	var prevNode workflow.Task = wflow.Start
	var currentNode workflow.Task
	for !end {
		switch prevNode.(type) {
		case *workflow.StartTask:
			nextNodeId := prevNode.GetNext()[0]
			currentNode, _ = wflow.Find(nextNodeId)
			u.AssertEquals(t, prevNode.(*workflow.StartTask).Next, currentNode.GetId())
		case *workflow.EndTask:
			end = true
		default: // currentNode = simple node
			nextNodeId := prevNode.GetNext()[0]
			currentNode, _ = wflow.Find(nextNodeId)
			u.AssertEquals(t, prevNode.(*workflow.SimpleTask).OutputTo, currentNode.GetId())
			u.AssertTrue(t, prevNode.(*workflow.SimpleTask).Func == f.Name)
		}
		prevNode = currentNode
	}
	u.AssertEquals(t, prevNode.(*workflow.EndTask), wflow.End)
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
	choiceWorkflow, found := wflow.Find(wflow.Start.Next)
	choice := choiceWorkflow.(*workflow.ChoiceTask)
	u.AssertTrue(t, found)
	for _, n := range wflow.Tasks {
		switch n.(type) {
		case *workflow.ChoiceTask:
			u.AssertEquals(t, len(choice.Conditions), len(choice.Alternatives))
			for _, s := range choice.Alternatives {
				simple, foundS := wflow.Find(s)
				u.AssertTrue(t, foundS)
				u.AssertEquals(t, simple.(*workflow.SimpleTask).OutputTo, wflow.End.GetId())
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

	choiceWorkflow, foundStartNext := wflow.Find(wflow.Start.Next)
	choice := choiceWorkflow.(*workflow.ChoiceTask)

	u.AssertNil(t, err)

	u.AssertNonNil(t, wflow.Start)
	u.AssertNonNil(t, wflow.End)
	u.AssertNonNil(t, wflow.Tasks)
	// u.AssertEquals(t, width+1, len(workflow.Tasks))

	// tasks := workflow.NewNodeSetFrom(workflow.Tasks)
	u.AssertTrue(t, foundStartNext)
	for _, n := range wflow.Tasks {
		switch node := n.(type) {
		case *workflow.ChoiceTask:
			u.AssertEquals(t, len(choice.Conditions), len(choice.Alternatives))
			for _, s := range choice.Alternatives {
				simple, foundS := wflow.Find(s)
				u.AssertTrue(t, foundS)
				if length == 1 {
					u.AssertEquals(t, simple.(*workflow.SimpleTask).OutputTo, wflow.End.GetId())
				}
			}
		case *workflow.SimpleTask:
			u.AssertTrue(t, node.Func == f.Name)
		}
	}
}

// TestBroadcastWorkflow verifies that a broadcast workflow is created correctly with fan out, simple nodes and fan in.
// All workflow branches have the same sequence of simple nodes.
func TestBroadcastWorkflow(t *testing.T) {
	//workflow.BranchNumber = 0

	m := make(map[string]interface{})
	m["input"] = 1
	width := 3
	length := 3
	f, fArr, err := initializeSameFunctionSlice(length, "js")
	u.AssertNil(t, err)

	wflow, errWorkflow := CreateBroadcastWorkflow(func() (*workflow.Workflow, error) { return CreateSequenceWorkflow(fArr...) }, width)
	u.AssertNil(t, errWorkflow)

	u.AssertNonNil(t, wflow.Start)
	u.AssertNonNil(t, wflow.End)
	u.AssertNonNil(t, wflow.Tasks)
	u.AssertEquals(t, length*width+4, len(wflow.Tasks)) // 1 (fanOut) + 1 (fanIn) + width * length (simpleNodes) + 1 start + 1 end

	// tasks := workflow.NewNodeSetFrom(workflow.Tasks)
	_, foundStartNext := wflow.Find(wflow.Start.Next)
	u.AssertTrue(t, foundStartNext)

	for _, n := range wflow.Tasks {
		switch n.(type) {
		case *workflow.FanOutTask:
			fanOut := n.(*workflow.FanOutTask)
			u.AssertEquals(t, len(fanOut.OutputTo), fanOut.FanOutDegree)
			u.AssertEquals(t, width, fanOut.FanOutDegree)
			for _, s := range fanOut.OutputTo {
				_, found := wflow.Find(s)
				u.AssertTrue(t, found)
			}
		case *workflow.FanInTask:
			fanIn := n.(*workflow.FanInTask)
			u.AssertEquals(t, width, fanIn.FanInDegree)
			u.AssertEquals(t, wflow.End.GetId(), fanIn.OutputTo)
		case *workflow.SimpleTask:
			u.AssertTrue(t, n.(*workflow.SimpleTask).Func == f.Name)
		default:
			continue
		}
	}
}

func TestScatterWorkflow(t *testing.T) {
	//workflow.BranchNumber = 0

	f, err := initializeExamplePyFunction()
	u.AssertNil(t, err)
	width := 3
	wflow, errWorkflow := CreateScatterSingleFunctionWorkflow(f, width)
	u.AssertNil(t, errWorkflow)

	u.AssertNonNil(t, wflow.Start)
	u.AssertNonNil(t, wflow.End)
	u.AssertNonNil(t, wflow.Tasks)
	u.AssertEquals(t, width+4, len(wflow.Tasks)) // 1 (fanOut) + 1 (fanIn) + width (simpleNodes) + 1 start + 1 end

	// tasks := workflow.NewNodeSetFrom(workflow.Tasks)
	startNext, startNextFound := wflow.Find(wflow.Start.Next)
	u.AssertTrue(t, startNextFound)
	_, ok := startNext.(*workflow.FanOutTask)
	u.AssertTrue(t, ok)
	simpleNodeChainedToFanIn := 0
	for _, n := range wflow.Tasks {
		switch node := n.(type) {
		case *workflow.FanOutTask:
			fanOut := node
			u.AssertEquals(t, len(fanOut.OutputTo), fanOut.FanOutDegree)
			u.AssertEquals(t, width, fanOut.FanOutDegree)
			for _, s := range fanOut.OutputTo {
				_, foundSimple := wflow.Find(s)
				u.AssertTrue(t, foundSimple)
			}
		case *workflow.FanInTask:
			fanIn := node
			u.AssertEquals(t, width, fanIn.FanInDegree)
			u.AssertEquals(t, wflow.End.GetId(), fanIn.OutputTo)
		case *workflow.SimpleTask:
			u.AssertTrue(t, n.(*workflow.SimpleTask).Func == f.Name)
			outputTo, _ := wflow.Find(node.OutputTo)
			_, chainedToFanIn := outputTo.(*workflow.FanInTask)
			u.AssertTrue(t, chainedToFanIn)
			simpleNodeChainedToFanIn++
		default:
			continue
		}
	}
	u.AssertEquals(t, width, simpleNodeChainedToFanIn)
}

func TestCreateBroadcastMultiFunctionWorkflow(t *testing.T) {
	//workflow.BranchNumber = 0

	length1 := 2
	f, fArrPy, err := initializeSameFunctionSlice(length1, "py")
	u.AssertNil(t, err)
	length2 := 3
	_, fArrJs, err2 := initializeSameFunctionSlice(length2, "js")
	u.AssertNil(t, err2)
	wflow, errWorkflow := CreateBroadcastMultiFunctionWorkflow(
		func() (*workflow.Workflow, error) { return CreateSequenceWorkflow(fArrPy...) },
		func() (*workflow.Workflow, error) { return CreateSequenceWorkflow(fArrJs...) },
	)
	u.AssertNil(t, errWorkflow)
	startNext, startNextFound := wflow.Find(wflow.Start.Next)

	u.AssertNonNil(t, wflow.Start)
	u.AssertNonNil(t, wflow.End)
	u.AssertNonNil(t, wflow.Tasks)
	u.AssertEquals(t, length1+length2+4, len(wflow.Tasks)) // 1 (fanOut) + 1 (fanIn) + width (simpleNodes) + 1 start + 1 end

	// tasks := workflow.NewNodeSetFrom(workflow.Tasks)
	u.AssertTrue(t, startNextFound)
	_, ok := startNext.(*workflow.FanOutTask)
	u.AssertTrue(t, ok)

	simpleNodeChainedToFanIn := 0
	for _, n := range wflow.Tasks {
		switch node := n.(type) {
		case *workflow.FanOutTask:
			fanOut := node
			u.AssertEquals(t, len(fanOut.OutputTo), fanOut.FanOutDegree)
			// test that there are simple nodes chained to fan out
			for _, s := range fanOut.OutputTo {
				_, foundSimple := wflow.Find(s)
				u.AssertTrue(t, foundSimple)
			}
		case *workflow.FanInTask:
			fanIn := node
			u.AssertEquals(t, wflow.End.GetId(), fanIn.OutputTo)
		default:
			continue
		case *workflow.SimpleTask:
			u.AssertTrue(t, node.Func == f.Name)
			outputTo, _ := wflow.Find(node.OutputTo)
			if _, ok := outputTo.(*workflow.FanInTask); ok {
				simpleNodeChainedToFanIn++
			}
		}
	}
	// test that the right number of simple nodes is chained to a fan in node.
	u.AssertEquals(t, 2, simpleNodeChainedToFanIn)
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
	//workflow.BranchNumber = 0

	f, err := initializeExamplePyFunction()
	u.AssertNil(t, err)
	width := 3
	wflow, err := workflow.NewBuilder().
		AddSimpleNode(f).
		AddChoiceNode(workflow.NewEqCondition(1, 4), workflow.NewDiffCondition(1, 4)).
		NextBranch(CreateSequenceWorkflow(f)).
		NextBranch(workflow.NewBuilder().
			AddScatterFanOutNode(width).
			ForEachParallelBranch(func() (*workflow.Workflow, error) { return CreateSequenceWorkflow(f) }).
			AddFanInNode().
			Build()).
		EndChoiceAndBuild()

	u.AssertNil(t, err)
	// tasks := workflow.NewNodeSetFrom(workflow.Tasks)
	simpleNodeChainedToFanIn := 0
	for _, n := range wflow.Tasks {
		switch node := n.(type) {
		case *workflow.FanOutTask:
			fanOut := node
			u.AssertEquals(t, len(fanOut.OutputTo), fanOut.FanOutDegree)
			u.AssertEquals(t, width, fanOut.FanOutDegree)
			for _, s := range fanOut.OutputTo {
				_, found := wflow.Find(s)
				u.AssertTrue(t, found)
			}
		case *workflow.FanInTask:
			fanIn := node
			u.AssertEquals(t, width, fanIn.FanInDegree)
			u.AssertEquals(t, wflow.End.GetId(), fanIn.OutputTo)
		case *workflow.SimpleTask:
			u.AssertTrue(t, node.Func == f.Name)
			nextNode, _ := wflow.Find(node.GetNext()[0])
			if _, ok := nextNode.(*workflow.FanInTask); ok {
				simpleNodeChainedToFanIn++
			}
		case *workflow.ChoiceTask:
			choice := node
			u.AssertEquals(t, len(choice.Conditions), len(choice.Alternatives))

			// specific for this test
			alt0, foundAlt0 := wflow.Find(choice.Alternatives[0])
			alt1, foundAlt1 := wflow.Find(choice.Alternatives[1])
			firstAlternative := alt0.(*workflow.SimpleTask)
			secondAlternative := alt1.(*workflow.FanOutTask)

			u.AssertTrue(t, foundAlt0)
			u.AssertTrue(t, foundAlt1)
			u.AssertEquals(t, firstAlternative.OutputTo, wflow.End.GetId())
			// checking fan out - simples - fan in
			for i := range secondAlternative.OutputTo {
				secondAltOutput, _ := wflow.Find(secondAlternative.OutputTo[i])
				simple, ok := secondAltOutput.(*workflow.SimpleTask)
				u.AssertTrue(t, ok)
				simpleNext, _ := wflow.Find(simple.OutputTo)
				_, okFanIn := simpleNext.(*workflow.FanInTask)
				u.AssertTrue(t, okFanIn)
			}

		default:
			continue
		}
	}
	u.AssertEquals(t, 3, simpleNodeChainedToFanIn)
}

func TestVisit(t *testing.T) {
	f, err := initializeExamplePyFunction()
	u.AssertNil(t, err)
	complexWorkflow, err := workflow.NewBuilder().
		AddSimpleNode(f).
		AddChoiceNode(workflow.NewEqCondition(1, 4), workflow.NewDiffCondition(1, 4)).
		NextBranch(CreateSequenceWorkflow(f)).
		NextBranch(workflow.NewBuilder().
			AddScatterFanOutNode(3).
			ForEachParallelBranch(func() (*workflow.Workflow, error) { return CreateSequenceWorkflow(f) }).
			AddFanInNode().
			Build()).
		EndChoiceAndBuild()
	u.AssertNil(t, err)

	startNext, _ := complexWorkflow.Find(complexWorkflow.Start.Next)

	choice := startNext.GetNext()[0]

	nodeList := make([]workflow.Task, 0)
	visitedNodes := workflow.Visit(complexWorkflow, complexWorkflow.Start.Id, nodeList, false)
	u.AssertEquals(t, len(complexWorkflow.Tasks), len(visitedNodes))

	visitedNodes = workflow.Visit(complexWorkflow, complexWorkflow.Start.Id, nodeList, true)
	u.AssertEquals(t, len(complexWorkflow.Tasks)-1, len(visitedNodes))

	visitedNodes = workflow.Visit(complexWorkflow, choice, nodeList, false)
	u.AssertEquals(t, 8, len(visitedNodes))

	visitedNodes = workflow.Visit(complexWorkflow, choice, nodeList, true)
	u.AssertEquals(t, 7, len(visitedNodes))

}
