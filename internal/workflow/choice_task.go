package workflow

import (
	"errors"
	"fmt"
	"github.com/grussorusso/serverledge/internal/types"
	"github.com/lithammer/shortuuid"
	"math"

	// "strconv"
	"strings"
)

// ChoiceNode receives one input and produces one result to one of two alternative nodes, based on condition
type ChoiceNode struct {
	Id           TaskId
	NodeType     TaskType
	BranchId     int
	Alternatives []TaskId
	Conditions   []Condition
}

func NewChoiceNode(conds []Condition) *ChoiceNode {
	return &ChoiceNode{
		Id:           TaskId(shortuuid.New()),
		NodeType:     Choice,
		Conditions:   conds,
		Alternatives: make([]TaskId, len(conds)),
	}
}

func (c *ChoiceNode) Equals(cmp types.Comparable) bool {
	switch cmp.(type) {
	case *ChoiceNode:
		c2 := cmp.(*ChoiceNode)
		if len(c.Conditions) != len(c2.Conditions) || len(c.Alternatives) != len(c2.Alternatives) {
			return false
		}
		for i := 0; i < len(c.Alternatives); i++ {
			if c.Alternatives[i] != c2.Alternatives[i] {
				return false
			}
			if !c.Conditions[i].Equals(c2.Conditions[i]) {
				return false
			}
		}
		return true
	default:
		return false
	}
}

// Exec for choice node evaluates the condition
func (c *ChoiceNode) Exec(_ *Request, params ...map[string]interface{}) (map[string]interface{}, error) {
	// TODO: ChoiceNode does not need this method
	return nil, nil
}

func (c *ChoiceNode) AddOutput(workflow *Workflow, taskId TaskId) error {

	if len(c.Alternatives) > len(c.Conditions) {
		return errors.New(fmt.Sprintf("there are %d alternatives but %d Conditions", len(c.Alternatives), len(c.Conditions)))
	}
	c.Alternatives = append(c.Alternatives, taskId)
	if len(c.Alternatives) > len(c.Conditions) {
		return errors.New(fmt.Sprintf("there are %d alternatives but %d Conditions", len(c.Alternatives), len(c.Conditions)))
	}
	return nil
}

func (c *ChoiceNode) CheckInput(input map[string]interface{}) error {
	return nil
}

func (c *ChoiceNode) PrepareOutput(workflow *Workflow, output map[string]interface{}) error {
	// TODO: ChoiceNode should not have this method
	return nil
}

// VisitBranch returns all node ids of a branch under a choice node; branch number starts from 0
func (c *ChoiceNode) VisitBranch(workflow *Workflow, branch int) []Task {
	branchNodes := make([]Task, 0)
	if len(c.Alternatives) <= branch {
		fmt.Printf("fail to get branch %d\n", branch)
		return branchNodes
	}
	node := c.Alternatives[branch]
	return Visit(workflow, node, branchNodes, true)
}

// GetNodesToSkip skips all node that are in a branch that will not be executed.
// If a skipped branch contains one or more node that is used by the current branch, the node,
// should NOT be skipped (Tested in TestParsingChoiceDagWithDataTestExpr)
func (c *ChoiceNode) GetNodesToSkip(workflow *Workflow, matchedCondition int) []Task {
	nodesToSkip := make([]Task, 0)

	nodesToNotSkip := c.VisitBranch(workflow, matchedCondition)
	for i := 0; i < len(c.Alternatives); i++ {
		if i == matchedCondition {
			continue
		}
		branchNodes := c.VisitBranch(workflow, i)
		for _, node := range branchNodes {
			shouldBeSkipped := true
			for _, nodeToNotSkip := range nodesToNotSkip {
				if node.Equals(nodeToNotSkip) {
					shouldBeSkipped = false
					break
				}
			}
			if shouldBeSkipped {
				nodesToSkip = append(nodesToSkip, node)
			}
		}
	}
	return nodesToSkip
}

func (c *ChoiceNode) GetNext() []TaskId {
	panic("cannot know the next task of a ChoiceTask in advance!")
}

func (c *ChoiceNode) Width() int {
	return len(c.Alternatives)
}

func (c *ChoiceNode) Name() string {
	n := len(c.Conditions)

	if n%2 == 0 {
		// se n =10 : -9 ---------
		// se n = 8 : -7 -------
		// se n = 6 : -5
		// se n = 4 : -3
		// se n = 2 : -1
		// [Simple|Simple|Simple|Simple|Simple|Simple|Simple|Simple|Simple|Simple]
		return strings.Repeat("-", 4*(n-1)-n/2) + "Choice" + strings.Repeat("-", 3*(n-1)+n/2)
	} else {
		pad := "-------"
		return strings.Repeat(pad, int(math.Max(float64(n/2), 0.))) + "Choice" + strings.Repeat(pad, int(math.Max(float64(n/2), 0.)))
	}
}

func (c *ChoiceNode) setBranchId(number int) {
	c.BranchId = number
}

func (c *ChoiceNode) GetBranchId() int {
	return c.BranchId
}

func (c *ChoiceNode) String() string {
	conditions := "<"
	for i, condFn := range c.Conditions {
		conditions += condFn.String()
		if i != len(c.Conditions) {
			conditions += " | "
		}
	}
	conditions += ">"
	return fmt.Sprintf("[ChoiceNode(%d): %s] ", len(c.Alternatives), conditions)
}

func (c *ChoiceNode) GetId() TaskId {
	return c.Id
}

func (c *ChoiceNode) GetNodeType() TaskType {
	return c.NodeType
}
