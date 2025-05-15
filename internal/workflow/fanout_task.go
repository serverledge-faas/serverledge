package workflow

import (
	"errors"
	"fmt"
	"github.com/lithammer/shortuuid"
	"github.com/serverledge-faas/serverledge/utils"
	"golang.org/x/exp/maps"
)

// FanOutTask is a Task that receives one input and sends multiple result, produced in parallel
type FanOutTask struct {
	baseTask
	FanOutDegree    int
	Mode            FanOutMode
	AssociatedFanIn TaskId
}
type FanOutMode int

const (
	Broadcast = iota
	Scatter
)

func NewFanOutTask(fanOutDegree int, fanOutType FanOutMode) *FanOutTask {
	return &FanOutTask{
		baseTask:     baseTask{Id: TaskId(shortuuid.New()), Type: FanOut},
		FanOutDegree: fanOutDegree,
		Mode:         fanOutType,
	}
}

// execute splits the output for the next parallel dags
// Scatter mode can only be used if the value held in the map is of type slice. Subdivides each map entry to a different task
// Broadcast mode can always be used. Copies the entire map to each of the subsequent tasks
func (f *FanOutTask) execute(progress *Progress, input *PartialData, r *Request) (*PartialData, *Progress, bool, error) {
	output := make(map[string]interface{})

	// input -> output: map["input":1] -> map["0":map["input":1], "1":map["input":1]]
	if f.Mode == Broadcast {
		for _, nextTask := range f.GetNext() {
			output[string(nextTask)] = maps.Clone(input.Data) // simply returns input, that will be copied to each subsequent task
		}
	} else if f.Mode == Scatter { // scatter only accepts an array with exactly fanOutDegree elements. However, multiple input values are allowed
		if len(input.Data) != 1 {
			return nil, progress, false, fmt.Errorf("failed to get one input for scatter fan out task: received %d inputs", len(input.Data))
		}
		inputName := maps.Keys(input.Data)[0]
		inputToScatter := input.Data[inputName]
		inputArrayToScatter, errNotSlice := utils.ConvertToSlice(inputToScatter)
		if errNotSlice != nil {
			return nil, progress, false, fmt.Errorf("cannot convert input %v to slice", inputToScatter)
		}

		if len(inputArrayToScatter) != f.FanOutDegree {
			return nil, progress, false, fmt.Errorf("input array size (%d) must be equal to fanOutDegree (%d). Check the previous task output",
				len(inputArrayToScatter), f.FanOutDegree)
		}

		for i, nextTask := range f.GetNext() {
			iOutput := make(map[string]interface{})
			iOutput[inputName] = inputArrayToScatter[i]
			output[string(nextTask)] = iOutput
		}
	} else {
		return nil, progress, false, fmt.Errorf("invalid fanout mode: %d", f.Mode)
	}

	/* using forTask = "" in order to create a special input to handle fanout
	 * case with Data field which contains a map[string]interface{} with the key set
	 * to taskId and the value which is also a map[string]interface{} containing the
	 * effective input for the nth-parallel task */
	// TODO: fix this
	outputData := NewPartialData(ReqId(r.Id), "", output)
	//newOutputDataMap := make(map[string]interface{})        // TODO: consider using a map of PartialData rather than a single PartialData object

	progress.Complete(f.GetId())
	for _, nextTask := range f.GetNext() {
		err := progress.AddReadyTask(nextTask)
		if err != nil {
			return nil, progress, false, err
		}
	}

	return outputData, progress, true, nil
}

func (f *FanOutTask) AddNext(nextTask Task) error {
	if len(f.NextTasks) == f.FanOutDegree {
		return errors.New("cannot add more output. Create a FanOutTask with a higher fanout degree")
	}
	return f.addNext(nextTask, false)
}

func (f *FanOutTask) String() string {
	outputs := ""
	for i, outputTo := range f.NextTasks {
		outputs += string(outputTo)
		if i != len(f.NextTasks)-1 {
			outputs += ", "
		}
	}
	outputs += "]"
	return fmt.Sprintf("[FanOutTask(%d)]->%s ", f.FanOutDegree, outputs)
}
