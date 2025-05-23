package workflow

import (
	"fmt"
	"github.com/lithammer/shortuuid"
)

// StartTask is a Task from which the execution of the Workflow starts. Invokes the first Task
type StartTask struct {
	baseTask
}

func NewStartTask() *StartTask {
	return &StartTask{
		baseTask: baseTask{
			Id:   TaskId(shortuuid.New()),
			Type: Start,
		},
	}
}

func (s *StartTask) SetNext(nextTask Task) error {
	s.NextTask = nextTask.GetId()
	return nil
}

func (s *StartTask) execute(progress *Progress, partialData *PartialData) (*PartialData, *Progress, bool, error) {

	progress.Complete(s.GetId())
	err := progress.AddReadyTask(s.GetNext())
	if err != nil {
		return nil, progress, false, err
	}
	return partialData, progress, true, nil
}

func (s *StartTask) String() string {
	return fmt.Sprintf("Start[%s]", s.GetId())
}
