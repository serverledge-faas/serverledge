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

	// SetNext connects the output of this task to another Task
	SetNext(nextTask Task) error

	GetType() TaskType

	GetNext() TaskId

	fmt.Stringer
	types.Comparable
}

type baseTask struct {
	Id       TaskId
	Type     TaskType
	NextTask TaskId
}

func (s *baseTask) GetNext() TaskId {
	return s.NextTask
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
