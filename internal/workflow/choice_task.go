package workflow

import (
	"errors"
	"fmt"
	"github.com/lithammer/shortuuid"
	"github.com/serverledge-faas/serverledge/internal/types"
	"math"

	"strings"
)

// ChoiceTask receives one input and produces one result to one of alternative tasks, based on condition
type ChoiceTask struct {
	Id           TaskId
	Type         TaskType
	Alternatives []TaskId
	Conditions   []Condition
}

func NewChoiceTask(conds []Condition) *ChoiceTask {
	return &ChoiceTask{
		Id:           TaskId(shortuuid.New()),
		Type:         Choice,
		Conditions:   conds,
		Alternatives: make([]TaskId, len(conds)),
	}
}

func (c *ChoiceTask) Equals(cmp types.Comparable) bool {
	switch cmp.(type) {
	case *ChoiceTask:
		c2 := cmp.(*ChoiceTask)
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

func (c *ChoiceTask) AddOutput(workflow *Workflow, taskId TaskId) error {

	if len(c.Alternatives) > len(c.Conditions) {
		return errors.New(fmt.Sprintf("there are %d alternatives but %d Conditions", len(c.Alternatives), len(c.Conditions)))
	}
	c.Alternatives = append(c.Alternatives, taskId)
	if len(c.Alternatives) > len(c.Conditions) {
		return errors.New(fmt.Sprintf("there are %d alternatives but %d Conditions", len(c.Alternatives), len(c.Conditions)))
	}
	return nil
}

func (c *ChoiceTask) execute(progress *Progress, input *PartialData, r *Request) (*PartialData, *Progress, bool, error) {

	outputData := NewPartialData(ReqId(r.Id), "", c.GetId(), nil) // partial initialization of outputData

	// NOTE: we do not call task.CheckInput() as this task has no signature to match against

	// simply evaluate the Conditions and set the matching one
	matchedCondition := -1
	for i, condition := range c.Conditions {
		ok, err := condition.Evaluate(input.Data)
		if err != nil {
			return nil, progress, false, fmt.Errorf("error while testing condition: %v", err)
		}
		if ok {
			matchedCondition = i
			break
		}
	}

	if matchedCondition < 0 {
		return nil, progress, false, fmt.Errorf("no condition is met")
	}

	nextTaskId := c.Alternatives[matchedCondition]
	outputData.ForTask = nextTaskId
	outputData.Data = input.Data

	// we skip all branch that will not be executed
	tasksToSkip := c.GetTasksToSkip(r.W, matchedCondition)
	for _, t := range tasksToSkip {
		progress.Skip(t.GetId())
	}

	progress.Complete(c.GetId())
	err := progress.AddReadyTask(nextTaskId)
	if err != nil {
		return nil, progress, false, err
	}
	return outputData, progress, true, nil
}

// VisitBranch returns all tasks of a branch under a choice task; branch number starts from 0
func (c *ChoiceTask) VisitBranch(workflow *Workflow, branch int) []Task {
	branchTasks := make([]Task, 0)
	if len(c.Alternatives) <= branch {
		fmt.Printf("fail to get branch %d\n", branch)
		return branchTasks
	}
	taskId := c.Alternatives[branch]
	return Visit(workflow, taskId, branchTasks, true)
}

// GetTasksToSkip skips all tasks that are in a branch that will not be executed.
// If a skipped branch contains one or more tasks that in use by the current branch, the task
// should NOT be skipped (Tested in TestParsingChoiceDagWithDataTestExpr)
func (c *ChoiceTask) GetTasksToSkip(workflow *Workflow, matchedCondition int) []Task {
	toSkip := make([]Task, 0)

	toNotSkip := c.VisitBranch(workflow, matchedCondition)
	for i := 0; i < len(c.Alternatives); i++ {
		if i == matchedCondition {
			continue
		}
		branchTasks := c.VisitBranch(workflow, i)
		for _, task := range branchTasks {
			shouldBeSkipped := true
			for _, taskNotSkipped := range toNotSkip {
				if task.Equals(taskNotSkipped) {
					shouldBeSkipped = false
					break
				}
			}
			if shouldBeSkipped {
				toSkip = append(toSkip, task)
			}
		}
	}
	return toSkip
}

func (c *ChoiceTask) GetNext() []TaskId {
	panic("cannot know the next task of a ChoiceTask in advance!")
}

func (c *ChoiceTask) Width() int {
	return len(c.Alternatives)
}

func (c *ChoiceTask) Name() string {
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

func (c *ChoiceTask) String() string {
	conditions := "<"
	for i, condFn := range c.Conditions {
		conditions += condFn.String()
		if i != len(c.Conditions) {
			conditions += " | "
		}
	}
	conditions += ">"
	return fmt.Sprintf("[ChoiceTask(%d): %s] ", len(c.Alternatives), conditions)
}

func (c *ChoiceTask) GetId() TaskId {
	return c.Id
}

func (c *ChoiceTask) GetType() TaskType {
	return c.Type
}
