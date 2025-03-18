package workflow

import (
	"fmt"
	"github.com/grussorusso/serverledge/internal/types"
	"github.com/lithammer/shortuuid"
)

// FanInNode receives and merges multiple input and produces a single result
type FanInNode struct {
	Id          TaskId
	NodeType    TaskType
	BranchId    int
	OutputTo    TaskId
	FanInDegree int
}

func NewFanInNode(fanInDegree int) *FanInNode {
	fanIn := FanInNode{
		Id:          TaskId(shortuuid.New()),
		NodeType:    FanIn,
		OutputTo:    "",
		FanInDegree: fanInDegree,
	}

	return &fanIn
}

func (f *FanInNode) Equals(cmp types.Comparable) bool {
	switch f1 := cmp.(type) {
	case *FanInNode:
		return f.Id == f1.Id && f.FanInDegree == f1.FanInDegree && f.OutputTo == f1.OutputTo
	default:
		return false
	}
}

// Exec already have all inputs when executing, so it simply merges them with the chosen policy
func (f *FanInNode) Exec(_ *Request, params ...map[string]interface{}) (map[string]interface{}, error) {
	output := make(map[string]interface{})

	for i, inputMap := range params {
		output[fmt.Sprintf("%d", i)] = inputMap
	}

	return output, nil
}

func (f *FanInNode) AddOutput(workflow *Workflow, taskId TaskId) error {
	f.OutputTo = taskId
	return nil
}

// CheckInput doesn't do anything for fan in node
func (f *FanInNode) CheckInput(input map[string]interface{}) error {
	return nil
}

func (f *FanInNode) PrepareOutput(workflow *Workflow, output map[string]interface{}) error {
	return nil // we should not do nothing, the output should be already ok
}

func (f *FanInNode) GetNext() []TaskId {
	// we only have one output
	return []TaskId{f.OutputTo}
}

func (f *FanInNode) Width() int {
	return f.FanInDegree
}

func (f *FanInNode) Name() string {
	return "Fan In"
}

func (f *FanInNode) String() string {
	return fmt.Sprintf("[FanInNode(%d)]", f.FanInDegree)
}

func (f *FanInNode) setBranchId(number int) {
	f.BranchId = number
}
func (f *FanInNode) GetBranchId() int {
	return f.BranchId
}

func (f *FanInNode) GetId() TaskId {
	return f.Id
}

func (f *FanInNode) GetNodeType() TaskType {
	return f.NodeType
}
