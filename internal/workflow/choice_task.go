package workflow

import (
	"errors"
	"fmt"
	"github.com/lithammer/shortuuid"
)

// ChoiceTask receives one input and produces one result to one of alternative tasks, based on condition
type ChoiceTask struct {
	baseTask
	Conditions           []Condition
	AlternativeNextTasks []TaskId
}

func NewChoiceTask(conds []Condition) *ChoiceTask {
	return &ChoiceTask{
		baseTask:   baseTask{Id: TaskId(shortuuid.New()), Type: Choice},
		Conditions: conds,
	}
}

func (c *ChoiceTask) AddAlternative(nextTask Task) error {
	if len(c.AlternativeNextTasks) >= len(c.Conditions) {
		return errors.New(fmt.Sprintf("there are %d alternatives but %d Conditions", len(c.AlternativeNextTasks), len(c.Conditions)))
	}
	c.AlternativeNextTasks = append(c.AlternativeNextTasks, nextTask.GetId())
	return nil
}

func (c *ChoiceTask) GetAlternatives() []TaskId {
	return c.AlternativeNextTasks
}

func (c *ChoiceTask) Evaluate(input *TaskData, r *Request) (TaskId, error) {

	// simply evaluate the Conditions and set the matching one
	matchedCondition := -1
	var extendedInputs = make(map[string]interface{})
	for k, v := range r.Params {
		extendedInputs[k] = v
	}
	for k, v := range input.Data {
		extendedInputs[k] = v
	}

	for i, condition := range c.Conditions {
		ok, err := condition.Evaluate(extendedInputs)
		if err != nil {
			return "", fmt.Errorf("error while testing condition: %v", err)
		}
		if ok {
			matchedCondition = i
			break
		}
	}

	if matchedCondition < 0 {
		return "", fmt.Errorf("no condition is met")
	}

	return c.AlternativeNextTasks[matchedCondition], nil
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
	return fmt.Sprintf("[ChoiceTask(%d): %s] ", len(c.AlternativeNextTasks), conditions)
}
