package workflow

import (
	"fmt"
	"github.com/lithammer/shortuuid"
	"github.com/serverledge-faas/serverledge/internal/types"
)

// FanInTask receives and merges multiple input and produces a single result
type FanInTask struct {
	Id          TaskId
	Type        TaskType
	OutputTo    TaskId
	FanInDegree int
}

func NewFanInTask(fanInDegree int) *FanInTask {
	fanIn := FanInTask{
		Id:          TaskId(shortuuid.New()),
		Type:        FanIn,
		OutputTo:    "",
		FanInDegree: fanInDegree,
	}

	return &fanIn
}

func (f *FanInTask) execute(progress *Progress, input *PartialData, r *Request) (*PartialData, *Progress, bool, error) {
	outputData := NewPartialData(ReqId(r.Id), f.GetNext()[0], f.GetId(), input.Data)
	progress.Complete(f.GetId())
	err := progress.AddReadyTask(f.GetNext()[0])
	if err != nil {
		return nil, progress, false, err
	}

	return outputData, progress, true, nil
}

func (f *FanInTask) Equals(cmp types.Comparable) bool {
	switch f1 := cmp.(type) {
	case *FanInTask:
		return f.Id == f1.Id && f.FanInDegree == f1.FanInDegree && f.OutputTo == f1.OutputTo
	default:
		return false
	}
}

func (f *FanInTask) AddOutput(workflow *Workflow, taskId TaskId) error {
	f.OutputTo = taskId
	return nil
}

func (f *FanInTask) GetNext() []TaskId {
	// we only have one output
	return []TaskId{f.OutputTo}
}

func (f *FanInTask) Width() int {
	return f.FanInDegree
}

func (f *FanInTask) Name() string {
	return "Fan In"
}

func (f *FanInTask) String() string {
	return fmt.Sprintf("[FanInTask(%d)]", f.FanInDegree)
}

func (f *FanInTask) GetId() TaskId {
	return f.Id
}

func (f *FanInTask) GetType() TaskType {
	return f.Type
}
