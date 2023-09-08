package fc

import (
	"fmt"
	"github.com/grussorusso/serverledge/internal/types"
	"github.com/lithammer/shortuuid"
)

// Reason can be used to parse the success or failure state of AWS State Language
type Reason int

const (
	Success Reason = iota
	Failure
)

// EndNode is a DagNode that represents the end of the Dag.
type EndNode struct {
	Id       DagNodeId
	NodeType DagNodeType
	Result   map[string]interface{}
}

func NewEndNode() *EndNode {
	return &EndNode{
		Id:       DagNodeId(shortuuid.New()),
		NodeType: End,
	}
}

func (e *EndNode) Equals(cmp types.Comparable) bool {
	switch cmp.(type) {
	case *EndNode:
		return true
	default:
		return false
	}
}

func (e *EndNode) Exec(*CompositionRequest, ...map[string]interface{}) (map[string]interface{}, error) {
	return e.Result, nil
}

func (e *EndNode) AddOutput(dag *Dag, dagNode DagNodeId) error {
	return nil // should not do anything. End node cannot be chained to anything
}

func (e *EndNode) CheckInput(input map[string]interface{}) error {
	e.Result = input
	return nil
}

// PrepareOutput doesn't need to do nothing for EndNode
func (e *EndNode) PrepareOutput(dag *Dag, output map[string]interface{}) error {
	return nil
}

func (e *EndNode) GetNext() []DagNodeId {
	// we return an empty array, because this is the EndNode
	return make([]DagNodeId, 0)
}

func (e *EndNode) Width() int {
	return 1
}

func (e *EndNode) Name() string {
	return " End  "
}

func (e *EndNode) ToString() string {
	return fmt.Sprintf("[EndNode]")
}
func (e *EndNode) setBranchId(number int) {
}
func (e *EndNode) GetBranchId() int {
	return 0
}

func (e *EndNode) GetId() DagNodeId {
	return e.Id
}

func (e *EndNode) GetNodeType() DagNodeType {
	return e.NodeType
}
