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

	// AddNext connects the output of this task to another Task
	AddNext(nextTask Task) error

	GetType() TaskType

	GetNext() []TaskId

	fmt.Stringer
	types.Comparable
}

type baseTask struct {
	Id        TaskId
	Type      TaskType
	NextTasks []TaskId
}

func (s *baseTask) GetNext() []TaskId {
	return s.NextTasks
}

func (s *baseTask) GetId() TaskId {
	return s.Id
}

func (s *baseTask) GetType() TaskType {
	return s.Type
}

func (e *baseTask) Equals(cmp types.Comparable) bool {
	e2, ok := cmp.(Task)
	if !ok {
		return false
	}

	return e.Id == e2.GetId() && e.Type == e2.GetType()
}

func (b *baseTask) addNext(nextTask Task, mustBeUnique bool) error {
	if mustBeUnique && b.NextTasks != nil && len(b.NextTasks) > 0 && nextTask.GetId() != b.NextTasks[0] {
		//log.Printf("task %s already has a next task (overwriting): %s - %s\n", b.Id, b.NextTasks[0], nextTask.GetId())
		b.NextTasks[0] = nextTask.GetId()
	} else {
		b.NextTasks = append(b.NextTasks, nextTask.GetId())
	}
	return nil
}
