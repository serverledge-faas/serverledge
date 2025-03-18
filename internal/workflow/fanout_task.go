package workflow

import (
	"errors"
	"fmt"
	"github.com/grussorusso/serverledge/internal/types"
	"github.com/grussorusso/serverledge/utils"
	"github.com/lithammer/shortuuid"
	"golang.org/x/exp/maps"
	"log"
	"math"
	"strings"
)

// TODO: when a branch has a fail node, all other branches should terminate immediately and the FanOut, FanIn and all nodes in the branches should be considered failed
// FanOutNode is a Task that receives one input and sends multiple result, produced in parallel
type FanOutNode struct {
	Id              TaskId
	NodeType        TaskType
	BranchId        int
	OutputTo        []TaskId
	FanOutDegree    int
	Type            FanOutType
	AssociatedFanIn TaskId
}
type FanOutType int

const (
	Broadcast = iota
	Scatter
)

func NewFanOutNode(fanOutDegree int, fanOutType FanOutType) *FanOutNode {
	return &FanOutNode{
		Id:           TaskId(shortuuid.New()),
		NodeType:     FanOut,
		OutputTo:     make([]TaskId, 0),
		FanOutDegree: fanOutDegree,
		Type:         fanOutType,
	}
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
func (f *FanOutNode) Exec(_ *Request, params ...map[string]interface{}) (map[string]interface{}, error) {
	output := make(map[string]interface{})

	// TODO: avoid forcing the interface implementation, so that the signature of Exec can be adapted
	if len(params) != 1 {
		return nil, fmt.Errorf("failed to get one input for choice node: received %d inputs", len(params))
	}

	// input -> output: map["input":1] -> map["0":map["input":1], "1":map["input":1]]
	if f.Type == Broadcast {
		for _, nextNode := range f.GetNext() {
			// TODO: check if a copy of params[0] is needed
			output[string(nextNode)] = params[0] // simply returns input, that will be copied to each subsequent node
		}
	} else if f.Type == Scatter { // scatter only accepts an array with exactly fanOutDegree elements. However, multiple input values are allowed
		inputName := maps.Keys(params[0])[0]
		inputToScatter := params[0][inputName]
		inputArrayToScatter, errNotSlice := utils.ConvertToSlice(inputToScatter)
		if errNotSlice != nil {
			return nil, fmt.Errorf("cannot convert input %v to slice", inputToScatter)
		}

		if len(inputArrayToScatter) != f.FanOutDegree {
			return nil, fmt.Errorf("input array size (%d) must be equal to fanOutDegree (%d). Check the previous node output",
				len(inputArrayToScatter), f.FanOutDegree)
		}

		for i, nextNode := range f.GetNext() {
			iOutput := make(map[string]interface{})
			iOutput[inputName] = inputArrayToScatter[i]
			output[string(nextNode)] = iOutput
		}
	} else {
		return nil, fmt.Errorf("invalid fanout mode: %d", f.Type)
	}

	return output, nil
}

func (f *FanOutNode) AddOutput(workflow *Workflow, taskId TaskId) error {
	if len(f.OutputTo) == f.FanOutDegree {
		return errors.New("cannot add more output. Create a FanOutNode with a higher fanout degree")
	}
	f.OutputTo = append(f.OutputTo, taskId)
	return nil
}

// CheckInput doesn't do anything for fanout node
func (f *FanOutNode) CheckInput(input map[string]interface{}) error {
	return nil
}

// PrepareOutput sends output to the next node in each parallel branch
func (f *FanOutNode) PrepareOutput(workflow *Workflow, output map[string]interface{}) error {
	// nothing to do
	return nil
}

// TODO: should return an error instead of an empty list, if OutputTo is nil or has less elements than fanOutDegree
func (f *FanOutNode) GetNext() []TaskId {
	if f.OutputTo == nil {
		log.Printf("You forgot to initialize OutputTo for FanOutNode\n")
		return []TaskId{}
	}

	if f.FanOutDegree != len(f.OutputTo) {
		log.Printf("The fanOutDegree and number of outputs does not match\n")
		return []TaskId{}
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

func (f *FanOutNode) String() string {
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

func (f *FanOutNode) GetId() TaskId {
	return f.Id
}
func (f *FanOutNode) GetNodeType() TaskType {
	return f.NodeType
}
