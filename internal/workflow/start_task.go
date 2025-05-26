package workflow

import (
	"fmt"
	"github.com/lithammer/shortuuid"
)

// StartTask is a Task from which the execution of the Workflow starts. Invokes the first Task
type StartTask struct {
	baseTask
	NextTask TaskId
}

func NewStartTask() *StartTask {
	return &StartTask{
		baseTask: baseTask{
			Id:   TaskId(shortuuid.New()),
			Type: Start,
		},
	}
}

func (s *StartTask) GetNext() TaskId {
	return s.NextTask
}

func (s *StartTask) SetNext(nextTask Task) error {
	s.NextTask = nextTask.GetId()
	return nil
}

func (s *StartTask) execute(input *PartialData, r *Request) (map[string]interface{}, error) {
	return input.Data, nil
}

func (s *StartTask) String() string {
	return fmt.Sprintf("Start[%s]", s.GetId())
}
