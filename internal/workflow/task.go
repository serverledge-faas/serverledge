package workflow

import (
	"fmt"

	"github.com/serverledge-faas/serverledge/internal/types"
)

type TaskId string

// Task is an interface for a single node in the Workflow
// all implementors must be pointers to a struct
type Task interface {
	types.Comparable
	Display
	AddOutput(workflow *Workflow, taskId TaskId) error
	GetType() TaskType
	GetNext() []TaskId
	Width() int // TODO: is this really needed?
}

type Display interface {
	fmt.Stringer
	GetId() TaskId
	Name() string
}

func Equals[D Task](d1 D, d2 D) bool {
	return d1.Equals(d2)
}
