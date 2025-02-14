package workflow

import (
	"fmt"
	"github.com/lithammer/shortuuid"
	"github.com/serverledge-faas/serverledge/internal/types"
)

type PassTask struct {
	Id         TaskId
	Type       TaskType
	Result     string
	ResultPath string
	OutputTo   TaskId
}

func NewPassTask(result string) *PassTask {
	passTask := PassTask{
		Id:     TaskId(shortuuid.New()),
		Type:   Pass,
		Result: result,
	}
	return &passTask
}

func (p *PassTask) Equals(cmp types.Comparable) bool {
	p2, ok := cmp.(*PassTask)
	if !ok {
		return false
	}
	return p.Id == p2.Id &&
		p.Type == p2.Type &&
		p.Result == p2.Result &&
		p.ResultPath == p2.ResultPath &&
		p.OutputTo == p2.OutputTo
}

// AddOutput for a PassTask connects it to another Task, except StartTask
func (p *PassTask) AddOutput(workflow *Workflow, taskId TaskId) error {
	_, ok := workflow.Tasks[taskId].(*StartTask)
	if ok {
		return fmt.Errorf("the PassTask cannot be chained to a startTask")
	}
	p.OutputTo = taskId
	return nil
}

func (p *PassTask) GetNext() []TaskId {
	return []TaskId{p.OutputTo}
}

func (p *PassTask) Width() int {
	return 1
}

func (p *PassTask) Name() string {
	return "Pass"
}

func (p *PassTask) String() string {
	return "[ Pass ]"
}

func (p *PassTask) GetId() TaskId {
	return p.Id
}

func (p *PassTask) GetType() TaskType {
	return p.Type
}
