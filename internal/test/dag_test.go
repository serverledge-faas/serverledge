package test

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"testing"

	"github.com/grussorusso/serverledge/internal/fc"
	u "github.com/grussorusso/serverledge/utils"
)

func TestDagMarshaling(t *testing.T) {
	f, _ := initializeExamplePyFunction()

	dag1, _ := fc.CreateEmptyDag()
	dag2, _ := fc.CreateSequenceDag(f, f, f)
	dag3, _ := fc.CreateChoiceDag(func() (*fc.Workflow, error) { return fc.CreateSequenceDag(f, f) })
	dag4, _ := fc.CreateBroadcastDag(func() (*fc.Workflow, error) { return fc.CreateSequenceDag(f, f) }, 4)
	dag5, _ := fc.CreateScatterSingleFunctionDag(f, 5)
	dag6, _ := fc.CreateBroadcastMultiFunctionDag(
		func() (*fc.Workflow, error) { return fc.CreateSequenceDag(f) },
		func() (*fc.Workflow, error) { return fc.CreateSequenceDag(f, f) },
		func() (*fc.Workflow, error) { return fc.CreateSequenceDag(f, f, f) },
	)
	dags := []*fc.Workflow{dag1, dag2, dag3, dag4, dag5, dag6}
	for i, workflow := range dags {
		marshal, errMarshal := json.Marshal(workflow)
		u.AssertNilMsg(t, errMarshal, "error during marshaling "+string(rune(i)))
		var retrieved fc.Workflow
		errUnmarshal := json.Unmarshal(marshal, &retrieved)
		u.AssertNilMsg(t, errUnmarshal, "failed composition unmarshal "+string(rune(i)))
		u.AssertTrueMsg(t, retrieved.Equals(workflow), fmt.Sprintf("retrieved workflow is not equal to initial workflow. Retrieved:\n%s,\nExpected:\n%s ", retrieved.String(), workflow.String()))
	}
}

// test for workflow connections
func TestEmptyDag(t *testing.T) {
	// fc.BranchNumber = 0

	input := 1
	m := make(map[string]interface{})
	m["input"] = input
	workflow, err := fc.CreateEmptyDag()
	u.AssertNil(t, err)

	u.AssertNonNil(t, workflow.Start)
	u.AssertNonNil(t, workflow.End)
	u.AssertEquals(t, workflow.Width, 1)
	u.AssertNonNil(t, workflow.Nodes)
	u.AssertEquals(t, workflow.Start.Next, workflow.End.GetId())
}

// TestSimpleDag creates a simple Workflow with one StartNode, two SimpleNode and one EndNode, executes it and gets the result.
func TestSimpleDag(t *testing.T) {
	//fc.BranchNumber = 0

	input := 1
	m := make(map[string]interface{})
	m["input"] = input
	length := 2

	f, fArr, err := initializeSameFunctionSlice(length, "js")
	u.AssertNil(t, err)

	workflow, err := fc.CreateSequenceDag(fArr...)
	u.AssertNil(t, err)

	u.AssertNonNil(t, workflow.Start)
	u.AssertNonNil(t, workflow.End)
	u.AssertEquals(t, workflow.Width, 1)
	u.AssertNonNil(t, workflow.Nodes)
	u.AssertEquals(t, len(workflow.Nodes)-2, length)

	// dagNodes := fc.NewNodeSetFrom(workflow.Nodes)
	_, found := workflow.Find(workflow.Start.Next)
	u.AssertTrue(t, found)
	end := false
	var prevNode fc.Task = workflow.Start
	var currentNode fc.Task
	for !end {
		switch prevNode.(type) {
		case *fc.StartNode:
			nextNodeId := prevNode.GetNext()[0]
			currentNode, _ = workflow.Find(nextNodeId)
			u.AssertEquals(t, prevNode.(*fc.StartNode).Next, currentNode.GetId())
		case *fc.EndNode:
			end = true
		default: // currentNode = simple node
			nextNodeId := prevNode.GetNext()[0]
			currentNode, _ = workflow.Find(nextNodeId)
			u.AssertEquals(t, prevNode.(*fc.SimpleNode).OutputTo, currentNode.GetId())
			u.AssertEquals(t, prevNode.(*fc.SimpleNode).BranchId, 0)
			u.AssertTrue(t, prevNode.(*fc.SimpleNode).Func == f.Name)
		}
		prevNode = currentNode
	}
	u.AssertEquals(t, prevNode.(*fc.EndNode), workflow.End)
}

func TestChoiceDag(t *testing.T) {
	//fc.BranchNumber = 0

	m := make(map[string]interface{})
	m["input"] = 1

	arr := make([]fc.Condition, 3)
	arr[0] = fc.NewConstCondition(false)
	arr[1] = fc.NewConstCondition(rand.Int()%2 == 0)
	arr[2] = fc.NewConstCondition(true)
	width := len(arr)
	f, fArr, err := initializeSameFunctionSlice(1, "js")
	u.AssertNil(t, err)

	workflow, err := fc.CreateChoiceDag(func() (*fc.Workflow, error) { return fc.CreateSequenceDag(fArr...) }, arr...)
	u.AssertNil(t, err)

	u.AssertNonNil(t, workflow.Start)
	u.AssertNonNil(t, workflow.End)
	u.AssertEquals(t, workflow.Width, width)
	u.AssertNonNil(t, workflow.Nodes)
	// u.AssertEquals(t, width+1, len(workflow.Nodes))

	//dagNodes := fc.NewNodeSetFrom(workflow.Nodes)
	choiceDag, found := workflow.Find(workflow.Start.Next)
	choice := choiceDag.(*fc.ChoiceNode)
	u.AssertTrue(t, found)
	for _, n := range workflow.Nodes {
		switch n.(type) {
		case *fc.ChoiceNode:
			u.AssertEquals(t, len(choice.Conditions), len(choice.Alternatives))
			for _, s := range choice.Alternatives {
				simple, foundS := workflow.Find(s)
				u.AssertTrue(t, foundS)
				u.AssertEquals(t, simple.(*fc.SimpleNode).OutputTo, workflow.End.GetId())
			}
			u.AssertEqualsMsg(t, 0, n.GetBranchId(), "wrong branchId for choice node")
		case *fc.SimpleNode:
			u.AssertTrue(t, n.(*fc.SimpleNode).Func == f.Name)
			for i, alternative := range choice.Alternatives {
				// the branch of the simple nodes should be 1,2 or 3 because branch of choice is 0
				if alternative == n.GetId() {
					u.AssertEqualsMsg(t, i+1, n.GetBranchId(), "wrong branchId for simple node")
				}
			}
		}
	}
}

func TestChoiceDag_BuiltWithNextBranch(t *testing.T) {
	//fc.BranchNumber = 0

	m := make(map[string]interface{})
	m["input"] = 1
	length := 2
	f, fArr, err := initializeSameFunctionSlice(length, "py")
	u.AssertNil(t, err)

	workflow, err := fc.NewDagBuilder().
		AddChoiceNode(
			fc.NewConstCondition(false),
			fc.NewSmallerCondition(2, 1),
			fc.NewConstCondition(true),
		).
		NextBranch(fc.CreateSequenceDag(fArr...)).
		NextBranch(fc.CreateSequenceDag(fArr...)).
		NextBranch(fc.CreateSequenceDag(fArr...)).
		EndChoiceAndBuild()

	choiceDag, foundStartNext := workflow.Find(workflow.Start.Next)
	choice := choiceDag.(*fc.ChoiceNode)
	width := len(choice.Alternatives)

	u.AssertNil(t, err)

	u.AssertNonNil(t, workflow.Start)
	u.AssertNonNil(t, workflow.End)
	u.AssertEquals(t, workflow.Width, width)
	u.AssertNonNil(t, workflow.Nodes)
	// u.AssertEquals(t, width+1, len(workflow.Nodes))

	// dagNodes := fc.NewNodeSetFrom(workflow.Nodes)
	u.AssertTrue(t, foundStartNext)
	for _, n := range workflow.Nodes {
		switch node := n.(type) {
		case *fc.ChoiceNode:
			u.AssertEquals(t, node.GetBranchId(), 0)
			u.AssertEquals(t, len(choice.Conditions), len(choice.Alternatives))
			for _, s := range choice.Alternatives {
				simple, foundS := workflow.Find(s)
				u.AssertTrue(t, foundS)
				if length == 1 {
					u.AssertEquals(t, simple.(*fc.SimpleNode).OutputTo, workflow.End.GetId())
				}
			}
		case *fc.SimpleNode:
			u.AssertTrue(t, node.Func == f.Name)
			for i, alternative := range choice.Alternatives {
				// the branch of the simple nodes should be 1,2 or 3 because branch of choice is 0
				if alternative == n.GetId() {
					u.AssertEqualsMsg(t, i+1, n.GetBranchId(), "wrong branchId for simple node")
				}
			}
			u.AssertTrue(t, node.GetBranchId() > 0)
		}
	}
}

// TestBroadcastDag verifies that a broadcast workflow is created correctly with fan out, simple nodes and fan in.
// All workflow branches have the same sequence of simple nodes.
func TestBroadcastDag(t *testing.T) {
	//fc.BranchNumber = 0

	m := make(map[string]interface{})
	m["input"] = 1
	width := 3
	length := 3
	f, fArr, err := initializeSameFunctionSlice(length, "js")
	u.AssertNil(t, err)

	workflow, errDag := fc.CreateBroadcastDag(func() (*fc.Workflow, error) { return fc.CreateSequenceDag(fArr...) }, width)
	u.AssertNil(t, errDag)

	u.AssertNonNil(t, workflow.Start)
	u.AssertNonNil(t, workflow.End)
	u.AssertEquals(t, width, workflow.Width)
	u.AssertNonNil(t, workflow.Nodes)
	u.AssertEquals(t, length*width+4, len(workflow.Nodes)) // 1 (fanOut) + 1 (fanIn) + width * length (simpleNodes) + 1 start + 1 end

	// dagNodes := fc.NewNodeSetFrom(workflow.Nodes)
	_, foundStartNext := workflow.Find(workflow.Start.Next)
	u.AssertTrue(t, foundStartNext)

	for _, n := range workflow.Nodes {
		switch n.(type) {
		case *fc.FanOutNode:
			fanOut := n.(*fc.FanOutNode)
			u.AssertEquals(t, len(fanOut.OutputTo), fanOut.FanOutDegree)
			u.AssertEquals(t, width, fanOut.FanOutDegree)
			for i, s := range fanOut.OutputTo {
				simple, found := workflow.Find(s)
				u.AssertTrue(t, found)
				u.AssertEquals(t, simple.GetBranchId(), i+1)
			}
			u.AssertEquals(t, n.GetBranchId(), 0)
		case *fc.FanInNode:
			fanIn := n.(*fc.FanInNode)
			u.AssertEquals(t, width, fanIn.FanInDegree)
			u.AssertEquals(t, workflow.End.GetId(), fanIn.OutputTo)
			u.AssertEquals(t, n.GetBranchId(), 4)
		case *fc.SimpleNode:
			u.AssertTrue(t, n.(*fc.SimpleNode).Func == f.Name)
			u.AssertTrue(t, n.GetBranchId() > 0 && n.GetBranchId() < 4)
		default:
			continue
		}
	}
}

func TestScatterDag(t *testing.T) {
	//fc.BranchNumber = 0

	f, err := initializeExamplePyFunction()
	u.AssertNil(t, err)
	width := 3
	workflow, errDag := fc.CreateScatterSingleFunctionDag(f, width)
	u.AssertNil(t, errDag)

	u.AssertNonNil(t, workflow.Start)
	u.AssertNonNil(t, workflow.End)
	u.AssertEquals(t, workflow.Width, width) // width is fixed at workflow definition-time
	u.AssertNonNil(t, workflow.Nodes)
	u.AssertEquals(t, width+4, len(workflow.Nodes)) // 1 (fanOut) + 1 (fanIn) + width (simpleNodes) + 1 start + 1 end

	// dagNodes := fc.NewNodeSetFrom(workflow.Nodes)
	startNext, startNextFound := workflow.Find(workflow.Start.Next)
	u.AssertTrue(t, startNextFound)
	_, ok := startNext.(*fc.FanOutNode)
	u.AssertTrue(t, ok)
	simpleNodeChainedToFanIn := 0
	for _, n := range workflow.Nodes {
		switch node := n.(type) {
		case *fc.FanOutNode:
			fanOut := node
			u.AssertEquals(t, len(fanOut.OutputTo), fanOut.FanOutDegree)
			u.AssertEquals(t, width, fanOut.FanOutDegree)
			for j, s := range fanOut.OutputTo {
				simple, foundSimple := workflow.Find(s)
				u.AssertTrue(t, foundSimple)
				u.AssertEquals(t, simple.GetBranchId(), j+1)
			}
			u.AssertEquals(t, node.GetBranchId(), 0)
		case *fc.FanInNode:
			fanIn := node
			u.AssertEquals(t, width, fanIn.FanInDegree)
			u.AssertEquals(t, workflow.End.GetId(), fanIn.OutputTo)
			u.AssertEquals(t, fanIn.GetBranchId(), fanIn.FanInDegree+1)
		case *fc.SimpleNode:
			u.AssertTrue(t, n.(*fc.SimpleNode).Func == f.Name)
			outputTo, _ := workflow.Find(node.OutputTo)
			_, chainedToFanIn := outputTo.(*fc.FanInNode)
			u.AssertTrue(t, chainedToFanIn)
			u.AssertTrue(t, n.GetBranchId() > 0 && n.GetBranchId() < 4)
			simpleNodeChainedToFanIn++
		default:
			continue
		}
	}
	u.AssertEquals(t, width, simpleNodeChainedToFanIn)
}

func TestCreateBroadcastMultiFunctionDag(t *testing.T) {
	//fc.BranchNumber = 0

	length1 := 2
	f, fArrPy, err := initializeSameFunctionSlice(length1, "py")
	u.AssertNil(t, err)
	length2 := 3
	_, fArrJs, err2 := initializeSameFunctionSlice(length2, "js")
	u.AssertNil(t, err2)
	workflow, errDag := fc.CreateBroadcastMultiFunctionDag(
		func() (*fc.Workflow, error) { return fc.CreateSequenceDag(fArrPy...) },
		func() (*fc.Workflow, error) { return fc.CreateSequenceDag(fArrJs...) },
	)
	u.AssertNil(t, errDag)
	startNext, startNextFound := workflow.Find(workflow.Start.Next)
	fanOutDegree := startNext.(*fc.FanOutNode).FanOutDegree

	u.AssertNonNil(t, workflow.Start)
	u.AssertNonNil(t, workflow.End)
	u.AssertEquals(t, 2, workflow.Width)
	u.AssertNonNil(t, workflow.Nodes)
	u.AssertEquals(t, length1+length2+4, len(workflow.Nodes)) // 1 (fanOut) + 1 (fanIn) + width (simpleNodes) + 1 start + 1 end

	// dagNodes := fc.NewNodeSetFrom(workflow.Nodes)
	u.AssertTrue(t, startNextFound)
	_, ok := startNext.(*fc.FanOutNode)
	u.AssertTrue(t, ok)

	simpleNodeChainedToFanIn := 0
	for _, n := range workflow.Nodes {
		switch node := n.(type) {
		case *fc.FanOutNode:
			fanOut := node
			u.AssertEquals(t, len(fanOut.OutputTo), fanOut.FanOutDegree)
			// test that there are simple nodes chained to fan out
			for _, s := range fanOut.OutputTo {
				simple, foundSimple := workflow.Find(s)
				u.AssertTrue(t, foundSimple)
				for i, branch := range fanOut.OutputTo {
					// the branch of the simple nodes should be 1,2 or 3 because branch of choice is 0
					if branch == simple.GetId() {
						u.AssertEqualsMsg(t, i+1, simple.GetBranchId(), "wrong branchId for simple node")
					}
				}
			}
			u.AssertEqualsMsg(t, 0, fanOut.GetBranchId(), "wrong branchId for fanOut")
		case *fc.FanInNode:
			fanIn := node
			u.AssertEquals(t, workflow.Width, fanIn.FanInDegree)
			u.AssertEquals(t, workflow.End.GetId(), fanIn.OutputTo)
			u.AssertEquals(t, fanIn.GetBranchId(), fanIn.FanInDegree+1)
		default:
			continue
		case *fc.SimpleNode:
			u.AssertTrue(t, node.Func == f.Name)
			u.AssertTrue(t, node.GetBranchId() > 0 && node.GetBranchId() < fanOutDegree+1)
			outputTo, _ := workflow.Find(node.OutputTo)
			if _, ok := outputTo.(*fc.FanInNode); ok {
				simpleNodeChainedToFanIn++
			}
		}
	}
	// test that the right number of simple nodes is chained to a fan in node.
	u.AssertEquals(t, 2, simpleNodeChainedToFanIn)
}

// TestDagBuilder tests a complex Workflow with every type of node in it
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
func TestDagBuilder(t *testing.T) {
	//fc.BranchNumber = 0

	f, err := initializeExamplePyFunction()
	u.AssertNil(t, err)
	width := 3
	workflow, err := fc.NewDagBuilder().
		AddSimpleNode(f).
		AddChoiceNode(fc.NewEqCondition(1, 4), fc.NewDiffCondition(1, 4)).
		NextBranch(fc.CreateSequenceDag(f)).
		NextBranch(fc.NewDagBuilder().
			AddScatterFanOutNode(width).
			ForEachParallelBranch(func() (*fc.Workflow, error) { return fc.CreateSequenceDag(f) }).
			AddFanInNode(fc.AddToArrayEntry).
			Build()).
		EndChoiceAndBuild()

	u.AssertNil(t, err)
	// dagNodes := fc.NewNodeSetFrom(workflow.Nodes)
	simpleNodeChainedToFanIn := 0
	for _, n := range workflow.Nodes {
		switch node := n.(type) {
		case *fc.FanOutNode:
			fanOut := node
			u.AssertEquals(t, len(fanOut.OutputTo), fanOut.FanOutDegree)
			u.AssertEquals(t, width, fanOut.FanOutDegree)
			u.AssertEqualsMsg(t, 2, fanOut.GetBranchId(), "fan out has wrong branchId")
			for _, s := range fanOut.OutputTo {
				_, found := workflow.Find(s)
				u.AssertTrue(t, found)
			}
		case *fc.FanInNode:
			fanIn := node
			u.AssertEquals(t, width, fanIn.FanInDegree)
			u.AssertEquals(t, workflow.End.GetId(), fanIn.OutputTo)
			u.AssertEqualsMsg(t, 6, fanIn.GetBranchId(), "wrong branchId for fan in")
		case *fc.SimpleNode:
			u.AssertTrue(t, node.Func == f.Name)
			nextNode, _ := workflow.Find(node.GetNext()[0])
			if _, ok := nextNode.(*fc.FanInNode); ok {
				simpleNodeChainedToFanIn++
				u.AssertTrueMsg(t, node.GetBranchId() > 2 && node.GetBranchId() < 6, "wrong branch for simple node connected to fanIn input") // the parallel branches of fan out node
			} else if _, ok2 := nextNode.(*fc.ChoiceNode); ok2 {
				u.AssertEqualsMsg(t, 0, node.GetBranchId(), "wrong branch for simpleNode connected to choice node input") // the first simple node
			} else if _, ok3 := nextNode.(*fc.EndNode); ok3 {
				u.AssertEqualsMsg(t, 1, node.GetBranchId(), "wrong branch for simpleNode connected to choice alternative 1") // the first branch of choice node
			} else {
				u.AssertTrueMsg(t, node.GetBranchId() > 2 && node.GetBranchId() < 6, "wrong branch for simple node inside parallel section") // the parallel branches of fan out node
			}
		case *fc.ChoiceNode:
			choice := node
			u.AssertEquals(t, len(choice.Conditions), len(choice.Alternatives))

			// specific for this test
			alt0, foundAlt0 := workflow.Find(choice.Alternatives[0])
			alt1, foundAlt1 := workflow.Find(choice.Alternatives[1])
			firstAlternative := alt0.(*fc.SimpleNode)
			secondAlternative := alt1.(*fc.FanOutNode)

			u.AssertTrue(t, foundAlt0)
			u.AssertTrue(t, foundAlt1)
			u.AssertEquals(t, firstAlternative.OutputTo, workflow.End.GetId())
			u.AssertEquals(t, choice.GetBranchId(), 0)
			// checking fan out - simples - fan in
			for i := range secondAlternative.OutputTo {
				secondAltOutput, _ := workflow.Find(secondAlternative.OutputTo[i])
				simple, ok := secondAltOutput.(*fc.SimpleNode)
				u.AssertTrue(t, ok)
				simpleNext, _ := workflow.Find(simple.OutputTo)
				_, okFanIn := simpleNext.(*fc.FanInNode)
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
	complexDag, err := fc.NewDagBuilder().
		AddSimpleNode(f).
		AddChoiceNode(fc.NewEqCondition(1, 4), fc.NewDiffCondition(1, 4)).
		NextBranch(fc.CreateSequenceDag(f)).
		NextBranch(fc.NewDagBuilder().
			AddScatterFanOutNode(3).
			ForEachParallelBranch(func() (*fc.Workflow, error) { return fc.CreateSequenceDag(f) }).
			AddFanInNode(fc.AddToArrayEntry).
			Build()).
		EndChoiceAndBuild()
	u.AssertNil(t, err)

	startNext, _ := complexDag.Find(complexDag.Start.Next)

	choice := startNext.GetNext()[0]

	nodeList := make([]fc.Task, 0)
	visitedNodes := fc.VisitDag(complexDag, complexDag.Start.Id, nodeList, false)
	u.AssertEquals(t, len(complexDag.Nodes), len(visitedNodes))

	visitedNodes = fc.VisitDag(complexDag, complexDag.Start.Id, nodeList, true)
	u.AssertEquals(t, len(complexDag.Nodes)-1, len(visitedNodes))

	visitedNodes = fc.VisitDag(complexDag, choice, nodeList, false)
	u.AssertEquals(t, 8, len(visitedNodes))

	visitedNodes = fc.VisitDag(complexDag, choice, nodeList, true)
	u.AssertEquals(t, 7, len(visitedNodes))

}
