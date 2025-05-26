package workflow

import (
	"fmt"
	"github.com/lithammer/shortuuid"
)

type SuccessTask struct {
	baseTask
	NextTask TaskId
}

func NewSuccessTask() *SuccessTask {
	return &SuccessTask{
		baseTask: baseTask{Id: TaskId(shortuuid.New()), Type: Succeed},
	}
}

func (s *SuccessTask) SetNext(nextTask Task) error {
	if nextTask.GetType() != End {
		return fmt.Errorf("the SuccessTask can only be chained to an end task")
	}
	s.NextTask = nextTask.GetId()
	return nil
}

func (s *SuccessTask) GetNext() TaskId {
	return s.NextTask
}

func (s *SuccessTask) String() string {
	return "[Succeed]"
}
