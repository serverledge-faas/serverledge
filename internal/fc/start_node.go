package fc

import (
	"errors"
	"fmt"
	"github.com/grussorusso/serverledge/internal/types"
	"github.com/lithammer/shortuuid"
	"reflect"
)

// StartNode is a DagNode from which the execution of the Dag starts. Invokes the first DagNode
type StartNode struct {
	Id       DagNodeId
	NodeType DagNodeType
	Next     DagNodeId
}

func NewStartNode() *StartNode {
	return &StartNode{
		Id:       DagNodeId(shortuuid.New()),
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

func (s *StartNode) AddOutput(dag *Dag, nodeId DagNodeId) error {
	node, found := dag.Find(nodeId)
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
func (s *StartNode) PrepareOutput(dag *Dag, output map[string]interface{}) error {
	nextNode, ok := dag.Find(s.Next)
	if !ok {
		return fmt.Errorf("node %s not found", s.Next)
	}
	err := nextNode.CheckInput(output)
	return err
}

func (s *StartNode) GetNext() []DagNodeId {
	// we only have one output
	return []DagNodeId{s.Next}
}

func (s *StartNode) Width() int {
	return 1
}

func (s *StartNode) Name() string {
	return "Start "
}

func (s *StartNode) ToString() string {
	return fmt.Sprintf("[%s]-next->%s", s.Name(), s.Next)
}

func (s *StartNode) setBranchId(number int) {
}
func (s *StartNode) GetBranchId() int {
	return 0
}

func (s *StartNode) GetId() DagNodeId {
	return s.Id
}

func (s *StartNode) GetNodeType() DagNodeType {
	return s.NodeType
}
