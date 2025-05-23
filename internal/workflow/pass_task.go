package workflow

import (
	"github.com/lithammer/shortuuid"
)

type PassTask struct {
	baseTask
	Result     string
	ResultPath string
}

func NewPassTask(result string) *PassTask {
	passTask := PassTask{
		baseTask: baseTask{Id: TaskId(shortuuid.New()), Type: Pass},
		Result:   result,
	}
	return &passTask
}

func (p *PassTask) SetNext(nextTask Task) error {
	p.NextTask = nextTask.GetId()
	return nil
}

func (p *PassTask) String() string {
	return "[ Pass ]"
}
