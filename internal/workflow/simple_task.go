package workflow

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/grussorusso/serverledge/internal/function"
	"github.com/grussorusso/serverledge/internal/node"
	"github.com/grussorusso/serverledge/internal/scheduling"
	"github.com/grussorusso/serverledge/internal/types"
	"github.com/labstack/gommon/log"
	"github.com/lithammer/shortuuid"
)

// SimpleNode is a Task that receives one input and sends one result
type SimpleNode struct {
	Id       TaskId
	NodeType TaskType
	BranchId int
	// input      map[string]interface{}
	OutputTo   TaskId
	Func       string
	inputMutex sync.Mutex // this is not marshaled
}

func NewSimpleNode(f string) *SimpleNode {
	return &SimpleNode{
		Id:         TaskId(shortuuid.New()),
		NodeType:   Simple,
		Func:       f,
		inputMutex: sync.Mutex{},
	}
}

func (s *SimpleNode) Exec(compRequest *Request, params ...map[string]interface{}) (map[string]interface{}, error) {
	funct, ok := function.GetFunction(s.Func)
	if !ok {
		return nil, fmt.Errorf("SimpleNode.function is null: you must initialize SimpleNode's function to execute it")
	}

	// the rest of the code is similar to a single function execution
	now := time.Now()
	requestId := fmt.Sprintf("%s-%s%d", s.Func, node.NodeIdentifier[len(node.NodeIdentifier)-5:], now.Nanosecond())

	r := &function.Request{
		ReqId:           requestId,
		Fun:             funct,
		Params:          params[0],
		Arrival:         now,
		ExecReport:      function.ExecutionReport{},
		RequestQoS:      compRequest.QoS,
		CanDoOffloading: true,
		Async:           false,
	}

	err := scheduling.SubmitRequest(r)
	if err != nil {
		return nil, err
	}

	outputData := make(map[string]interface{})

	// extract output map
	for _, o := range funct.Signature.GetOutputs() {
		var result map[string]interface{}
		//if the output is a struct/map, we should return a map with struct field and values
		errNotUnmarshable := json.Unmarshal([]byte(r.ExecReport.Result), &result)
		if errNotUnmarshable != nil {
			// if the output is a simple type (e.g. int, bool, string, array) we simply add it to the map

			outputData[o.Name] = r.ExecReport.Result
			err1 := o.CheckOutput(outputData)
			if err1 != nil {
				return nil, fmt.Errorf("output type checking failed: %v", err1)
			}
			outputData[o.Name], err1 = o.TryParse(r.ExecReport.Result)
			if err1 != nil {
				return nil, fmt.Errorf("failed to parse intermediate output: %v", err1)
			}
		} else {
			val, found := result[o.Name]
			if !found {
				return nil, fmt.Errorf("failed to find result with name %s", o.Name)
			}
			outputData[o.Name] = val

		}

	}

	r.ExecReport.Result = fmt.Sprintf("%v", outputData)
	// saving execution report for this function
	//compRequest.ExecReport.Reports[CreateExecutionReportId(s)] = &r.ExecReport
	compRequest.ExecReport.Reports.Set(CreateExecutionReportId(s), &r.ExecReport)
	/*
		cs := ""
		if !r.ExecReport.IsWarmStart {
			cs = fmt.Sprintf("- cold start: %v", !r.ExecReport.IsWarmStart)
		}
		fmt.Printf("Function Request %s - result of simple node %s: %v %s\n", r.Id, s.Id, r.ExecReport.Result, cs)
	*/
	return outputData, nil
}

func (s *SimpleNode) Equals(cmp types.Comparable) bool {
	switch cmp.(type) {
	case *SimpleNode:
		s2 := cmp.(*SimpleNode)
		idOk := s.Id == s2.Id
		// inputOk := s.InputFrom == s2.InputFrom
		funcOk := s.Func == s2.Func
		outputOk := s.OutputTo == s2.OutputTo
		return idOk && funcOk && outputOk // && inputOk
	default:
		return false
	}
}

// AddOutput connects the output of the SimpleNode to another Task
func (s *SimpleNode) AddOutput(workflow *Workflow, taskId TaskId) error {
	s.OutputTo = taskId
	return nil
}

func (s *SimpleNode) CheckInput(input map[string]interface{}) error {
	funct, exists := function.GetFunction(s.Func)
	if !exists {
		return fmt.Errorf("funtion %s doesn't exists", s.Func)
	}

	if funct.Signature == nil {
		return fmt.Errorf("signature of function %s is nil. Recreate the function with a valid signature.\n", funct.Name)
	}

	return funct.Signature.CheckAllInputs(input)
}

// PrepareOutput is used to send the output to the following function and if needed can be used to modify the SimpleNode output representation, like OutputPath
func (s *SimpleNode) PrepareOutput(workflow *Workflow, output map[string]interface{}) error {
	funct, exists := function.GetFunction(s.Func) // we are getting the function from cache if not already downloaded
	if !exists {
		return fmt.Errorf("funtion %s doesn't exists", s.Func)
	}
	if funct.Signature == nil {
		funct.Signature = function.SignatureInference(output)
		log.Warnf("signature of function %s is nil. Output map: %v\n", funct.Name, output)
	}
	err := funct.Signature.CheckAllOutputs(output)
	if err != nil {
		return fmt.Errorf("error while checking outputs: %v", err)
	}
	// get signature of next nodes, if present and maps the output there

	// we have only one output node
	task, _ := workflow.Find(s.GetNext()[0])

	switch nodeType := task.(type) {
	case *SimpleNode:
		return nodeType.MapOutput(output) // needed to convert type of data from one node to the next so that its signature type-checks
	case *ChoiceNode:
		return nodeType.MapOutput(output, *funct.Signature) // needed to convert type of data from one node to the next so that its signature type-checks
	}

	return nil
}

// MapOutput transforms the output for the next simpleNode, to make it compatible with its Signature
func (s *SimpleNode) MapOutput(output map[string]interface{}) error {
	funct, exists := function.GetFunction(s.Func)
	if !exists {
		return fmt.Errorf("function %s doesn't exist", s.Func)
	}
	sign := funct.Signature
	// if there are no inputs, we do nothing
	for _, def := range sign.GetInputs() {
		// if output has same name as input, we do not need to change name
		_, present := output[def.Name]
		if present {
			continue
		}
		// find an entry in the output map that successfully checks the type of the InputDefinition
		key, ok := def.FindEntryThatTypeChecks(output)
		if ok {
			// we get the output value
			val := output[key]
			// we remove the output entry ...
			delete(output, key)
			// and replace with the input entry
			output[def.Name] = val
			// save the output map in the input of the node
			//s.inputMutex.Lock()
			//s.input = output
			//s.inputMutex.Unlock()
		} else {
			// otherwise if no one of the entry typechecks we are doomed
			return fmt.Errorf("no output entry input-checks with the next function")
		}
	}
	// if the outputs are more than the needed input, we do nothing
	return nil
}

func (s *SimpleNode) GetNext() []TaskId {
	// we only have one output
	return []TaskId{s.OutputTo}
}

func (s *SimpleNode) Width() int {
	return 1
}
func (s *SimpleNode) Name() string {
	return "Simple"
}

func (s *SimpleNode) String() string {
	return fmt.Sprintf("[SimpleNode (%s) func %s()]->%s", s.Id, s.Func, s.OutputTo)
}

func (s *SimpleNode) setBranchId(number int) {
	s.BranchId = number
}
func (s *SimpleNode) GetBranchId() int {
	return s.BranchId
}

func (s *SimpleNode) GetId() TaskId {
	return s.Id
}

func (s *SimpleNode) GetNodeType() TaskType {
	return s.NodeType
}
