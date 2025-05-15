package workflow

import (
	"errors"
	"fmt"
	"github.com/lithammer/shortuuid"
)

// ChoiceTask receives one input and produces one result to one of alternative tasks, based on condition
type ChoiceTask struct {
	baseTask
	Conditions []Condition
}

func NewChoiceTask(conds []Condition) *ChoiceTask {
	return &ChoiceTask{
		baseTask:   baseTask{Id: TaskId(shortuuid.New()), Type: Choice},
		Conditions: conds,
	}
}

func (c *ChoiceTask) AddNext(nextTask Task) error {
	if len(c.NextTasks) >= len(c.Conditions) {
		return errors.New(fmt.Sprintf("there are %d alternatives but %d Conditions", len(c.NextTasks), len(c.Conditions)))
	}
	return c.addNext(nextTask, false)
}

func (c *ChoiceTask) execute(progress *Progress, input *PartialData, r *Request) (*PartialData, *Progress, bool, error) {

	outputData := NewPartialData(ReqId(r.Id), c.GetId(), nil) // partial initialization of outputData

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

	nextTaskId := c.NextTasks[matchedCondition]
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
	if len(c.NextTasks) <= branch {
		fmt.Printf("fail to get branch %d\n", branch)
		return branchTasks
	}
	taskId := c.NextTasks[branch]
	return Visit(workflow, taskId, branchTasks, true)
}

// GetTasksToSkip skips all tasks that are in a branch that will not be executed.
// If a skipped branch contains one or more tasks that in use by the current branch, the task
// should NOT be skipped (Tested in TestParsingChoiceDagWithDataTestExpr)
func (c *ChoiceTask) GetTasksToSkip(workflow *Workflow, matchedCondition int) []Task {
	toSkip := make([]Task, 0)

	toNotSkip := c.VisitBranch(workflow, matchedCondition)
	for i := 0; i < len(c.NextTasks); i++ {
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

func (c *ChoiceTask) String() string {
	conditions := "<"
	for i, condFn := range c.Conditions {
		conditions += condFn.String()
		if i != len(c.Conditions) {
			conditions += " | "
		}
	}
	conditions += ">"
	return fmt.Sprintf("[ChoiceTask(%d): %s] ", len(c.NextTasks), conditions)
}
