package fc

import (
	"errors"
	"fmt"
	"reflect"

	"github.com/grussorusso/serverledge/internal/types"
	"github.com/lithammer/shortuuid"
)

// StartNode is a Task from which the execution of the Workflow starts. Invokes the first Task
type StartNode struct {
	Id       TaskId
	NodeType DagNodeType
	Next     TaskId
}

func NewStartNode() *StartNode {
	return &StartNode{
		Id:       TaskId(shortuuid.New()),
		NodeType: Start,
	}
}

func (s *StartNode) Equals(cmp types.Comparable) bool {
	switch cmp.(type) {
	case *StartNode:
		return s.Next == cmp.(*StartNode).Next
	default:
		return false
	}
}

func (s *StartNode) AddOutput(workflow *Workflow, nodeId TaskId) error {
	node, found := workflow.Find(nodeId)
	if !found {
		return fmt.Errorf("node %s not found", nodeId)
	}
	switch node.(type) {
	case *StartNode:
		return errors.New(fmt.Sprintf("you cannot add an result of type %s to a %s", reflect.TypeOf(node), reflect.TypeOf(s)))
	default:
		s.Next = nodeId
	}
	return nil
}

func (s *StartNode) Exec(*CompositionRequest, ...map[string]interface{}) (map[string]interface{}, error) {
	panic("you can't exec a start node")
}

// CheckInput does nothing for StartNode
func (s *StartNode) CheckInput(input map[string]interface{}) error {
	return nil
}

// PrepareOutput for StartNode just send to the next node what it receives
func (s *StartNode) PrepareOutput(workflow *Workflow, output map[string]interface{}) error {
	nextNode, ok := workflow.Find(s.Next)
	if !ok {
		return fmt.Errorf("node %s not found", s.Next)
	}
	err := nextNode.CheckInput(output)
	return err
}

func (s *StartNode) GetNext() []TaskId {
	// we only have one output
	return []TaskId{s.Next}
}

func (s *StartNode) Width() int {
	return 1
}

func (s *StartNode) Name() string {
	return "Start "
}

func (s *StartNode) String() string {
	return fmt.Sprintf("[%s]-next->%s", s.Name(), s.Next)
}

func (s *StartNode) setBranchId(number int) {
}
func (s *StartNode) GetBranchId() int {
	return 0
}

func (s *StartNode) GetId() TaskId {
	return s.Id
}

func (s *StartNode) GetNodeType() DagNodeType {
	return s.NodeType
}
