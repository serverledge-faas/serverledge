package workflow

import (
	"fmt"
	"github.com/lithammer/shortuuid"
)

type FailureTask struct {
	baseTask
	Error    string
	Cause    string
	NextTask TaskId
}

func NewFailureTask(error, cause string) *FailureTask {
	fail := FailureTask{
		baseTask: baseTask{Id: TaskId(shortuuid.New()), Type: Fail},
		Error:    error,
		Cause:    cause,
	}
	return &fail
}

func (f *FailureTask) GetNext() TaskId {
	return f.NextTask
}

func (f *FailureTask) SetNext(nextTask Task) error {
	if nextTask.GetType() != End {
		return fmt.Errorf("the Fail can only be chained to an end task")
	}
	f.NextTask = nextTask.GetId()
	return nil
}

func (f *FailureTask) String() string {
	return fmt.Sprintf("[Fail: %s]", f.Error)
}

func (f *FailureTask) execute(input *PartialData, r *Request) (map[string]interface{}, error) {
	output := make(map[string]interface{})
	output[f.Error] = f.Cause
	return output, nil
}
