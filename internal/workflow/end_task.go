package workflow

import (
	"fmt"

	"github.com/lithammer/shortuuid"
)

// EndTask is a Task that represents the end of the Workflow.
type EndTask struct {
	baseTask
}

func NewEndTask() *EndTask {
	return &EndTask{
		baseTask: baseTask{Id: TaskId(shortuuid.New()), Type: End},
	}
}

func (e *EndTask) String() string {
	return fmt.Sprintf("[EndTask]")
}
