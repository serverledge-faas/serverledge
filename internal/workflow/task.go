package workflow

import (
	"fmt"
	"github.com/serverledge-faas/serverledge/internal/types"
)

type TaskId string

// Task is an interface for a single node in the Workflow
// all implementors must be pointers to a struct
type Task interface {
	GetId() TaskId

	GetType() TaskType

	fmt.Stringer
	types.Comparable
}

type UnaryTask interface {
	Task

	GetNext() TaskId

	// SetNext connects the output of this task to another Task
	SetNext(nextTask Task) error

	execute(data *PartialData, r *Request) (map[string]interface{}, error)
}

type ConditionalTask interface {
	Task
	AddAlternative(nextTask Task) error
	GetAlternatives() []TaskId
	Evaluate(data *PartialData, r *Request) (TaskId, error)
}

type baseTask struct {
	Id   TaskId
	Type TaskType
}

func (s *baseTask) GetId() TaskId {
	return s.Id
}

func (s *baseTask) GetType() TaskType {
	return s.Type
}

func (s *baseTask) Equals(cmp types.Comparable) bool {
	e2, ok := cmp.(Task)
	if !ok {
		return false
	}

	return s.Id == e2.GetId() && s.Type == e2.GetType()
}
