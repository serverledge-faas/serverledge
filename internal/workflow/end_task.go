package workflow

import (
	"fmt"

	"github.com/lithammer/shortuuid"
	"github.com/serverledge-faas/serverledge/internal/types"
)

// EndTask is a Task that represents the end of the Workflow.
type EndTask struct {
	Id     TaskId
	Type   TaskType
	Result map[string]interface{}
}

func NewEndTask() *EndTask {
	return &EndTask{
		Id:     TaskId(shortuuid.New()),
		Type:   End,
		Result: make(map[string]interface{}),
	}
}

func (e *EndTask) Equals(cmp types.Comparable) bool {
	e2, ok := cmp.(*EndTask)
	if !ok {
		return false
	}

	if len(e.Result) != len(e2.Result) {
		return false
	}

	for k := range e.Result {
		if e.Result[k] != e2.Result[k] {
			return false
		}
	}

	return e.Id == e2.Id && e.Type == e2.Type
}

func (e *EndTask) execute(progress *Progress, partialData *PartialData) (*PartialData, *Progress, bool, error) {
	progress.Complete(e.Id)
	return partialData, progress, false, nil // false because we want to stop when reaching the end
}

func (e *EndTask) AddOutput(workflow *Workflow, taskId TaskId) error {
	return nil // should not do anything. End node cannot be chained to anything
}

func (e *EndTask) GetNext() []TaskId {
	// we return an empty array, because this is the EndTask
	return make([]TaskId, 0)
}

func (e *EndTask) Width() int {
	return 1
}

func (e *EndTask) Name() string {
	return " End  "
}

func (e *EndTask) String() string {
	return fmt.Sprintf("[EndTask]")
}
func (e *EndTask) setBranchId(number int) {
}
func (e *EndTask) GetBranchId() int {
	return 0
}

func (e *EndTask) GetId() TaskId {
	return e.Id
}

func (e *EndTask) GetType() TaskType {
	return e.Type
}
