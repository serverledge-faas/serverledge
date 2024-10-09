package fc

import (
	"fmt"

	"github.com/grussorusso/serverledge/internal/types"
	"github.com/lithammer/shortuuid"
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
		Result:   make(map[string]interface{}),
	}
}

func (e *EndNode) Equals(cmp types.Comparable) bool {
	e2, ok := cmp.(*EndNode)
	if !ok {
		return false
	}

	if len(e.Result) != len(e2.Result) {
		return false
	}

	for k := range e.Result {
		if e.Result[k] != e2.Result[k] {
			return false
		}
	}

	return e.Id == e2.Id && e.NodeType == e2.NodeType
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

func (e *EndNode) String() string {
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
