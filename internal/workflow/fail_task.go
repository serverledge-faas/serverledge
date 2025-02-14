package workflow

import (
	"fmt"
	"github.com/lithammer/shortuuid"
	"github.com/serverledge-faas/serverledge/internal/types"
)

type FailureTask struct {
	Id    TaskId
	Type  TaskType
	Error string
	Cause string

	OutputTo TaskId
}

func NewFailureTask(error, cause string) *FailureTask {
	if len(error) > 20 {
		fmt.Printf("error string identifier should be less than 20 characters but is %d characters long\n", len(error))
	}
	fail := FailureTask{
		Id:    TaskId(shortuuid.New()),
		Type:  Fail,
		Error: error,
		Cause: cause,
	}
	return &fail
}

func (f *FailureTask) Equals(cmp types.Comparable) bool {
	f2, ok := cmp.(*FailureTask)
	if !ok {
		return false
	}
	return f.Id == f2.Id &&
		f.Type == f2.Type &&
		f.Error == f2.Error &&
		f.Cause == f2.Cause &&
		f.OutputTo == f2.OutputTo
}

func (f *FailureTask) execute(progress *Progress, r *Request) (*PartialData, *Progress, bool, error) {

	output := make(map[string]interface{})
	output[f.Error] = f.Cause
	outputData := NewPartialData(ReqId(r.Id), f.GetNext()[0], f.GetId(), output)

	progress.Complete(f.GetId())

	shouldContinueExecution := f.GetType() != Fail && f.GetType() != Succeed
	return outputData, progress, shouldContinueExecution, nil
}

func (f *FailureTask) AddOutput(workflow *Workflow, taskId TaskId) error {
	_, ok := workflow.Tasks[taskId].(*EndTask)
	if !ok {
		return fmt.Errorf("the Fail can only be chained to an end task")
	}
	f.OutputTo = taskId
	return nil
}

func (f *FailureTask) GetNext() []TaskId {
	return []TaskId{f.OutputTo}
}

func (f *FailureTask) Width() int {
	return 1
}

func (f *FailureTask) Name() string {
	return " Fail "
}

func (f *FailureTask) String() string {
	return fmt.Sprintf("[Fail: %s]", f.Error)
}

func (f *FailureTask) GetId() TaskId {
	return f.Id
}

func (f *FailureTask) GetType() TaskType {
	return f.Type
}
