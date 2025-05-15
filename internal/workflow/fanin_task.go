package workflow

import (
	"fmt"
	"github.com/lithammer/shortuuid"
)

// FanInTask receives and merges multiple input and produces a single result
type FanInTask struct {
	baseTask
	FanInDegree int
}

func NewFanInTask(fanInDegree int) *FanInTask {
	fanIn := FanInTask{
		baseTask:    baseTask{Id: TaskId(shortuuid.New()), Type: FanIn},
		FanInDegree: fanInDegree,
	}

	return &fanIn
}

func (f *FanInTask) execute(progress *Progress, input *PartialData, r *Request) (*PartialData, *Progress, bool, error) {
	outputData := NewPartialData(ReqId(r.Id), f.GetNext()[0], input.Data)
	progress.Complete(f.GetId())
	err := progress.AddReadyTask(f.GetNext()[0])
	if err != nil {
		return nil, progress, false, err
	}

	return outputData, progress, true, nil
}

func (f *FanInTask) AddNext(nextTask Task) error {
	return f.addNext(nextTask, true)
}

func (f *FanInTask) String() string {
	return fmt.Sprintf("[FanInTask(%d)]", f.FanInDegree)
}
