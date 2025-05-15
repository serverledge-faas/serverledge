package workflow

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/lithammer/shortuuid"
	"github.com/serverledge-faas/serverledge/internal/function"
	"github.com/serverledge-faas/serverledge/internal/node"
	"github.com/serverledge-faas/serverledge/internal/scheduling"
)

// SimpleTask is a Task that receives one input and sends one result
type SimpleTask struct {
	baseTask
	Func string
}

func NewSimpleTask(f string) *SimpleTask {
	return &SimpleTask{
		baseTask: baseTask{Id: TaskId(shortuuid.New()), Type: Simple},
		Func:     f,
	}
}

func (s *SimpleTask) execute(progress *Progress, input *PartialData, r *Request) (*PartialData, *Progress, bool, error) {

	err := s.CheckInput(input.Data)
	if err != nil {
		return nil, progress, false, err
	}
	output, err := s.exec(r, input.Data)
	if err != nil {
		return nil, progress, false, err
	}

	nextTask := s.GetNext()[0]
	outputData := NewPartialData(ReqId(r.Id), nextTask, output)

	progress.Complete(s.Id)
	err = progress.AddReadyTask(nextTask)
	if err != nil {
		return nil, progress, false, err
	}
	return outputData, progress, true, nil
}

func (s *SimpleTask) exec(compRequest *Request, params ...map[string]interface{}) (map[string]interface{}, error) {
	funct, ok := function.GetFunction(s.Func)
	if !ok {
		return nil, fmt.Errorf("SimpleTask.function is null: you must initialize SimpleTask's function to execute it")
	}

	// the rest of the code is similar to a single function execution
	now := time.Now()
	requestId := fmt.Sprintf("%s-%s%d", s.Func, node.NodeIdentifier[len(node.NodeIdentifier)-5:], now.Nanosecond())

	r := &function.Request{
		Fun:             funct,
		Params:          params[0],
		Arrival:         now,
		RequestQoS:      compRequest.QoS,
		CanDoOffloading: true,
		Async:           false,
	}
	r.Ctx = context.WithValue(context.Background(), "ReqId", requestId)

	report, err := scheduling.SubmitRequest(r)
	if err != nil {
		return nil, err
	}

	outputData := make(map[string]interface{})

	var result map[string]interface{}
	//if the output is a struct/map, we should return a map with struct field and values
	err = json.Unmarshal([]byte(report.Result), &result)
	if err != nil {
		// if the output is a simple type (e.g. int, bool, string, array) we simply add it to the map
		if len(funct.Signature.GetOutputs()) != 1 {
			return nil, fmt.Errorf("single value returned (%v), more than 1 expected", report.Result)
		}
		output := funct.Signature.GetOutputs()[0]
		outputData[output.Name], err = output.TryParse(report.Result)
		if err != nil {
			return nil, fmt.Errorf("failed to parse intermediate output: %v", err)
		}
	} else {
		for _, o := range funct.Signature.GetOutputs() {
			val, found := result[o.Name]
			if !found {
				return nil, fmt.Errorf("failed to find result with name %s", o.Name)
			}
			outputData[o.Name] = val
			err = o.CheckOutput(outputData)
			if err != nil {
				return nil, fmt.Errorf("output type checking failed: %v", err)
			}
		}
	}

	// saving execution report for this function
	compRequest.ExecReport.Reports[CreateExecutionReportId(s)] = &report

	return outputData, nil
}

func (s *SimpleTask) AddNext(nextTask Task) error {
	return s.addNext(nextTask, true)
}

func (s *SimpleTask) CheckInput(input map[string]interface{}) error {
	funct, exists := function.GetFunction(s.Func)
	if !exists {
		return fmt.Errorf("funtion %s doesn't exists", s.Func)
	}

	if funct.Signature == nil {
		return fmt.Errorf("signature of function %s is nil. Recreate the function with a valid signature.\n", funct.Name)
	}

	return funct.Signature.CheckOrMatchInputs(input)
}

func (s *SimpleTask) String() string {
	return fmt.Sprintf("[SimpleTask (%s) func %s()]->%v", s.Id, s.Func, s.NextTasks)
}
