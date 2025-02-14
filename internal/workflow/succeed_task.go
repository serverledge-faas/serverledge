package workflow

import (
	"fmt"
	"github.com/lithammer/shortuuid"
	"github.com/serverledge-faas/serverledge/internal/types"
)

type SuccessTask struct {
	Id         TaskId
	Type       TaskType
	InputPath  string
	OutputPath string
	OutputTo   TaskId
}

func NewSuccessTask() *SuccessTask {
	return &SuccessTask{
		Id:   TaskId(shortuuid.New()),
		Type: Succeed,
	}
}

func (s *SuccessTask) Equals(cmp types.Comparable) bool {
	s2, ok := cmp.(*SuccessTask)
	if !ok {
		return false
	}
	return s.Id == s2.Id &&
		s.Type == s2.Type &&
		s.InputPath == s2.InputPath &&
		s.OutputPath == s2.OutputPath &&
		s.OutputTo == s2.OutputTo
}

func (s *SuccessTask) AddOutput(workflow *Workflow, taskId TaskId) error {
	_, ok := workflow.Tasks[taskId].(*EndTask)
	if !ok {
		return fmt.Errorf("the SuccessTask can only be chained to an end task")
	}
	s.OutputTo = taskId
	return nil
}

func (s *SuccessTask) GetNext() []TaskId {
	return []TaskId{s.OutputTo}
}

func (s *SuccessTask) Width() int {
	return 1
}

func (s *SuccessTask) Name() string {
	return "Success"
}

func (s *SuccessTask) String() string {
	return "[Succeed]"
}

func (s *SuccessTask) GetId() TaskId {
	return s.Id
}

func (s *SuccessTask) GetType() TaskType {
	return s.Type
}
