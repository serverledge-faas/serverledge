package workflow

import (
	"errors"
	"fmt"
	"reflect"

	"github.com/lithammer/shortuuid"
	"github.com/serverledge-faas/serverledge/internal/types"
)

// StartTask is a Task from which the execution of the Workflow starts. Invokes the first Task
type StartTask struct {
	Id   TaskId
	Type TaskType
	Next TaskId
}

func NewStartTask() *StartTask {
	return &StartTask{
		Id:   TaskId(shortuuid.New()),
		Type: Start,
	}
}

func (s *StartTask) Equals(cmp types.Comparable) bool {
	switch cmp.(type) {
	case *StartTask:
		return s.Next == cmp.(*StartTask).Next
	default:
		return false
	}
}

func (s *StartTask) AddOutput(workflow *Workflow, taskId TaskId) error {
	task, found := workflow.Find(taskId)
	if !found {
		return fmt.Errorf("task %s not found", taskId)
	}
	switch task.(type) {
	case *StartTask:
		return errors.New(fmt.Sprintf("you cannot add an result of type %s to a %s", reflect.TypeOf(task), reflect.TypeOf(s)))
	default:
		s.Next = taskId
	}
	return nil
}

func (s *StartTask) execute(progress *Progress, partialData *PartialData) (*PartialData, *Progress, bool, error) {

	progress.Complete(s.GetId())
	err := progress.AddReadyTask(s.GetNext()[0])
	if err != nil {
		return nil, progress, false, err
	}
	return partialData, progress, true, nil
}

func (s *StartTask) GetNext() []TaskId {
	// we only have one output
	return []TaskId{s.Next}
}

func (s *StartTask) Width() int {
	return 1
}

func (s *StartTask) Name() string {
	return "Start "
}

func (s *StartTask) String() string {
	return fmt.Sprintf("[%s]-next->%s", s.Name(), s.Next)
}

func (s *StartTask) GetId() TaskId {
	return s.Id
}

func (s *StartTask) GetType() TaskType {
	return s.Type
}
