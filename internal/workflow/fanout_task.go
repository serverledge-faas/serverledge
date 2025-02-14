package workflow

import (
	"errors"
	"fmt"
	"github.com/lithammer/shortuuid"
	"github.com/serverledge-faas/serverledge/internal/types"
	"github.com/serverledge-faas/serverledge/utils"
	"golang.org/x/exp/maps"
	"log"
	"math"
	"strings"
)

// FanOutTask is a Task that receives one input and sends multiple result, produced in parallel
type FanOutTask struct {
	Id              TaskId
	Type            TaskType
	OutputTo        []TaskId
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
		Id:           TaskId(shortuuid.New()),
		Type:         FanOut,
		OutputTo:     make([]TaskId, 0),
		FanOutDegree: fanOutDegree,
		Mode:         fanOutType,
	}
}

func (f *FanOutTask) Equals(cmp types.Comparable) bool {
	switch cmp.(type) {
	case *FanOutTask:
		f2 := cmp.(*FanOutTask)
		for i := 0; i < len(f.OutputTo); i++ {
			if f.OutputTo[i] != f2.OutputTo[i] {
				return false
			}
		}
		return f.FanOutDegree == f2.FanOutDegree
	default:
		return false
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
	outputData := NewPartialData(ReqId(r.Id), "", f.GetId(), output)
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

func (f *FanOutTask) AddOutput(workflow *Workflow, taskId TaskId) error {
	if len(f.OutputTo) == f.FanOutDegree {
		return errors.New("cannot add more output. Create a FanOutTask with a higher fanout degree")
	}
	f.OutputTo = append(f.OutputTo, taskId)
	return nil
}

// TODO: should return an error instead of an empty list, if OutputTo is nil or has less elements than fanOutDegree
func (f *FanOutTask) GetNext() []TaskId {
	if f.OutputTo == nil {
		log.Printf("You forgot to initialize OutputTo for FanOutTask\n")
		return []TaskId{}
	}

	if f.FanOutDegree != len(f.OutputTo) {
		log.Printf("The fanOutDegree and number of outputs does not match\n")
		return []TaskId{}
	}

	return f.OutputTo
}

func (f *FanOutTask) Width() int {
	return f.FanOutDegree
}

func (f *FanOutTask) Name() string {
	n := f.FanOutDegree
	if n%2 == 0 {
		return strings.Repeat("-", 4*(n-1)-n/2) + "FanOut" + strings.Repeat("-", 3*(n-1)+n/2)
	} else {
		pad := "-------"
		return strings.Repeat(pad, int(math.Max(float64(n/2), 0.))) + "FanOut" + strings.Repeat(pad, int(math.Max(float64(n/2), 0.)))
	}
}

func (f *FanOutTask) String() string {
	outputs := ""
	for i, outputTo := range f.OutputTo {
		outputs += string(outputTo)
		if i != len(f.OutputTo)-1 {
			outputs += ", "
		}
	}
	outputs += "]"
	return fmt.Sprintf("[FanOutTask(%d)]->%s ", f.FanOutDegree, outputs)
}

func (f *FanOutTask) GetId() TaskId {
	return f.Id
}
func (f *FanOutTask) GetType() TaskType {
	return f.Type
}
