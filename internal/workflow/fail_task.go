package workflow

import (
	"fmt"
	"github.com/lithammer/shortuuid"
)

type FailureTask struct {
	baseTask
	Error string
	Cause string
}

func NewFailureTask(error, cause string) *FailureTask {
	fail := FailureTask{
		baseTask: baseTask{Id: TaskId(shortuuid.New()), Type: Fail},
		Error:    error,
		Cause:    cause,
	}
	return &fail
}

func (f *FailureTask) execute(progress *Progress, r *Request) (*PartialData, *Progress, bool, error) {

	output := make(map[string]interface{})
	output[f.Error] = f.Cause
	outputData := NewPartialData(ReqId(r.Id), f.GetNext()[0], output)

	progress.Complete(f.GetId())

	shouldContinueExecution := f.GetType() != Fail && f.GetType() != Succeed
	return outputData, progress, shouldContinueExecution, nil
}

func (f *FailureTask) AddNext(nextTask Task) error {
	if nextTask.GetType() != End {
		return fmt.Errorf("the Fail can only be chained to an end task")
	}
	return f.addNext(nextTask, true)
}

func (f *FailureTask) String() string {
	return fmt.Sprintf("[Fail: %s]", f.Error)
}
