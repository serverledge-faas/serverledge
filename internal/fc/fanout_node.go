package fc

import (
	"errors"
	"fmt"
	"github.com/grussorusso/serverledge/internal/function"
	"github.com/grussorusso/serverledge/internal/types"
	"github.com/lithammer/shortuuid"
	"log"
	"math"
	"strings"
	"time"
)

// FanOutNode is a DagNode that receives one input and sends multiple result, produced in parallel
type FanOutNode struct {
	Id              DagNodeId
	NodeType        DagNodeType
	BranchId        int
	input           map[string]interface{}
	OutputTo        []DagNodeId
	FanOutDegree    int
	Type            FanOutType
	AssociatedFanIn DagNodeId
}
type FanOutType int

const (
	Broadcast = iota
	Scatter
)

type ScatterMode int

const (
	RoundRobin                    = iota // copies each map entry in an unordered, round-robin manner, so that more or less all branches have the same number of input
	SplitEqually                         // gets the length of the map, divides by the number of parallel branches and sends balances input to each branch. If the division is not integer-based, the last branches receive less input
	OneMapEntryForEachBranch             // Given N branch, gives one entry in an unordered manner to one branch. If there are more entries than branches, the remaining entries are discarded. When there are too many branches, some of them receive an empty input map
	OneMapArrayEntryForEachBranch        // Given N branch, gives each array entry in an ordered manner to one branch. When there are more entries than branches, the remaining entries are discarded. When there are too many branches, some of them receive an empty input map
)

func NewFanOutNode(fanOutDegree int, fanOutType FanOutType) *FanOutNode {
	return &FanOutNode{
		Id:           DagNodeId(shortuuid.New()),
		NodeType:     FanOut,
		OutputTo:     make([]DagNodeId, 0),
		FanOutDegree: fanOutDegree,
		Type:         fanOutType,
	}
}

func (f *FanOutNode) getBranchNumbers(dag *Dag) []int {
	branchNumbers := make([]int, f.FanOutDegree)
	for i, o := range f.OutputTo {
		nod, _ := dag.Find(o)
		branchNumbers[i] = nod.GetBranchId()
	}
	return branchNumbers
}

func (f *FanOutNode) Equals(cmp types.Comparable) bool {
	switch cmp.(type) {
	case *FanOutNode:
		f2 := cmp.(*FanOutNode)
		for i := 0; i < len(f.OutputTo); i++ {
			if f.OutputTo[i] != f2.OutputTo[i] {
				return false
			}
		}
		return f.FanOutDegree == f2.FanOutDegree
	default:
		return false
	}
}

// Exec splits the output for the next parallel dags
// Scatter mode can only be used if the value held in the map is of type slice. Subdivides each map entry to a different node
// Broadcast mode can always be used. Copies the entire map to each of the subsequent nodes
func (f *FanOutNode) Exec(compRequest *CompositionRequest) (map[string]interface{}, error) {
	var output map[string]interface{} = nil
	var err error = nil
	t0 := time.Now()
	// input -> output: map["input":1] -> map["0":map["input":1], "1":map["input":1]]
	if f.Type == Broadcast {
		broadcast := make(map[string]interface{})
		for i := 0; i < f.FanOutDegree; i++ {
			broadcast[fmt.Sprintf("%d", i)] = f.input // simply returns input, that will be copied to each subsequent node
		}
		output = broadcast
	} else if f.Type == Scatter { // scatter only accepts a single array with exactly fanOutDegree elements
		// get inputs
		if len(f.input) != 1 {
			err = fmt.Errorf("invalid fanout input size, should only accept one array, but has %d different inputs", len(f.input))
		} else {
			for inputName, inputToScatter := range f.input {
				inputArrayToScatter := inputToScatter.([]any)

				if len(inputArrayToScatter) != f.FanOutDegree {
					err = fmt.Errorf("input array size (%d) must be equal to fanOutDegree (%d). Check the previous node output", len(inputArrayToScatter), f.FanOutDegree)
					break
				}

				scatter := make(map[string]interface{})
				for i := 0; i < f.FanOutDegree; i++ {
					scatter[fmt.Sprintf("%d", i)] = inputArrayToScatter[i]
				}
				output[inputName] = scatter
				// there is only one element, so we break now for safety
				break
			}
		}
	} else {
		output = nil
		err = fmt.Errorf("invalid fanout mode, valid values are 0=Broadcast and 1=Scatter")
	}
	respAndDuration := time.Now().Sub(t0).Seconds()
	compRequest.ExecReport.Reports.Set(CreateExecutionReportId(f), &function.ExecutionReport{
		Result:         fmt.Sprintf("%v", output),
		ResponseTime:   respAndDuration,
		IsWarmStart:    true, // not in a container
		InitTime:       0,
		OffloadLatency: 0,
		Duration:       respAndDuration,
		SchedAction:    "",
	})
	return output, err
}

func (f *FanOutNode) AddOutput(dag *Dag, dagNode DagNodeId) error {
	if len(f.OutputTo) == f.FanOutDegree {
		return errors.New("cannot add more output. Create a FanOutNode with a higher fanout degree")
	}
	f.OutputTo = append(f.OutputTo, dagNode)
	return nil
}

func (f *FanOutNode) ReceiveInput(input map[string]interface{}) error {
	f.input = input
	return nil
}

// PrepareOutput sends output to the next node in each parallel branch
func (f *FanOutNode) PrepareOutput(dag *Dag, output map[string]interface{}) error {
	for i, nodeId := range f.GetNext() {
		outputNode, ok := dag.Find(nodeId)
		if !ok {
			return fmt.Errorf("FanoutNode.PrepareOutput: cannot find node")
		}
		if f.Type == Broadcast {
			err := outputNode.ReceiveInput(output)
			if err != nil {
				return err
			}
		} else if f.Type == Scatter {
			mapForBranch, ok := output[fmt.Sprintf("%d", i)].(map[string]interface{})
			if !ok {
				return fmt.Errorf("FanOutNode.PrepareOutput: failed to convert output interface{} to map[string]interface{}")
			}
			err := outputNode.ReceiveInput(mapForBranch)
			if err != nil {
				return err
			}
			return nil
		} else {
			return fmt.Errorf("invalid argument")
		}
	}
	return nil
}

func (f *FanOutNode) GetNext() []DagNodeId {
	// we have multiple outputs
	if f.FanOutDegree <= 1 {
		log.Printf("You should have used a SimpleNode or EndNode for fanOutDegree less than 2\n")
		return []DagNodeId{}
	}

	if f.OutputTo == nil {
		log.Printf("You forgot to initialize OutputTo for FanOutNode\n")
		return []DagNodeId{}
	}

	if f.FanOutDegree != len(f.OutputTo) {
		log.Printf("The fanOutDegree and number of outputs does not match\n")
		return []DagNodeId{}
	}

	return f.OutputTo
}

func (f *FanOutNode) Width() int {
	return f.FanOutDegree
}

func (f *FanOutNode) Name() string {
	n := f.FanOutDegree
	if n%2 == 0 {
		return strings.Repeat("-", 4*(n-1)-n/2) + "FanOut" + strings.Repeat("-", 3*(n-1)+n/2)
	} else {
		pad := "-------"
		return strings.Repeat(pad, int(math.Max(float64(n/2), 0.))) + "FanOut" + strings.Repeat(pad, int(math.Max(float64(n/2), 0.)))
	}
}

func (f *FanOutNode) ToString() string {
	outputs := ""
	for i, outputTo := range f.OutputTo {
		outputs += string(outputTo)
		if i != len(f.OutputTo)-1 {
			outputs += ", "
		}
	}
	outputs += "]"
	return fmt.Sprintf("[FanOutNode(%d)]->%s ", f.FanOutDegree, outputs)
}

func (f *FanOutNode) setBranchId(number int) {
	f.BranchId = number
}

func (f *FanOutNode) GetBranchId() int {
	return f.BranchId
}

func (f *FanOutNode) GetId() DagNodeId {
	return f.Id
}
func (f *FanOutNode) GetNodeType() DagNodeType {
	return f.NodeType
}
