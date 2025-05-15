package workflow

import (
	"fmt"
	"github.com/lithammer/shortuuid"
)

type SuccessTask struct {
	baseTask
}

func NewSuccessTask() *SuccessTask {
	return &SuccessTask{
		baseTask: baseTask{Id: TaskId(shortuuid.New()), Type: Succeed},
	}
}

func (s *SuccessTask) AddNext(nextTask Task) error {
	if nextTask.GetType() != End {
		return fmt.Errorf("the SuccessTask can only be chained to an end task")
	}
	return s.addNext(nextTask, true)
}

func (s *SuccessTask) String() string {
	return "[Succeed]"
}
