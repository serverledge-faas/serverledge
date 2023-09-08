package fc

import (
	"encoding/json"
	"fmt"
	"github.com/grussorusso/serverledge/internal/function"
	"github.com/grussorusso/serverledge/internal/node"
	"github.com/grussorusso/serverledge/internal/scheduling"
	"github.com/grussorusso/serverledge/internal/types"
	"github.com/lithammer/shortuuid"
	"sync"
	"time"
)

var compositionRequestsPool = sync.Pool{
	New: func() any {
		return new(function.Request)
	},
}

// SimpleNode is a DagNode that receives one input and sends one result
type SimpleNode struct {
	Id       DagNodeId
	NodeType DagNodeType
	BranchId int
	// input      map[string]interface{}
	OutputTo   DagNodeId
	Func       string
	inputMutex sync.Mutex // this is not marshaled
}

func NewSimpleNode(f string) *SimpleNode {
	return &SimpleNode{
		Id:         DagNodeId(shortuuid.New()),
		NodeType:   Simple,
		Func:       f,
		inputMutex: sync.Mutex{},
	}
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

func (s *SimpleNode) Exec(compRequest *CompositionRequest, params ...map[string]interface{}) (map[string]interface{}, error) {
	funct, ok := function.GetFunction(s.Func)
	if !ok {
		return nil, fmt.Errorf("SimpleNode.function is null: you must initialize SimpleNode's function to execute it")
	}
	/*
		// this is for debug
		s.inputMutex.Lock()
		fmt.Printf("executing simple node %s for request %s with input %v\n", s.Id, compRequest.ReqId, s.input)
		s.inputMutex.Unlock()
	*/
	// creates the function if not exists. Maybe someone deleted by accident the function before starting the dag.
	if !funct.Exists() {
		errNotSaved := funct.SaveToEtcd()
		return nil, fmt.Errorf("the function %s cannot be saved while trying to exec the function composition %v", s.Func, errNotSaved)
	}
	// the rest of the code is similar to a single function execution
	now := time.Now()
	requestId := fmt.Sprintf("%s-%s%d", s.Func, node.NodeIdentifier[len(node.NodeIdentifier)-5:], now.Nanosecond())
	s.inputMutex.Lock()
	r := &function.Request{
		ReqId:   requestId,
		Fun:     funct,
		Params:  params[0],
		Arrival: now,
		ExecReport: function.ExecutionReport{
			SchedAction:    "",
			OffloadLatency: 0.0,
		},
		RequestQoS:      compRequest.RequestQoSMap[s.Func],
		CanDoOffloading: true,
		Async:           false,
		IsInComposition: true,
	}
	s.inputMutex.Unlock()

	// executes the function, waiting for the result
	err := scheduling.SubmitRequest(r)
	if err != nil {
		return nil, err
	}

	m := make(map[string]interface{})
	firstOutputName := ""
	// extract output map
	for i, o := range funct.Signature.GetOutputs() {
		if i == 0 {
			firstOutputName = o.Name
		}
		var result map[string]interface{}
		//if the output is a struct/map, we should return a map with struct field and values
		errNotUnmarshable := json.Unmarshal([]byte(r.ExecReport.Result), &result)
		if errNotUnmarshable != nil {
			// if the output is a simple type (e.g. int, bool, string, array) we simply add it to the map
			m[o.Name] = r.ExecReport.Result
			err1 := o.CheckOutput(m)
			if err1 != nil {
				return nil, fmt.Errorf("output type checking failed: %v", err1)
			}
			m[o.Name], err1 = o.TryParse(r.ExecReport.Result)
			if err1 != nil {
				return nil, fmt.Errorf("failed to parse intermediate output: %v", err1)
			}
		} else {
			val, found := result[o.Name]
			if !found {
				return nil, fmt.Errorf("failed to find result with name %s", o.Name)
			}
			m[o.Name] = val
		}

	}
	if len(m) == 1 {
		r.ExecReport.Result = fmt.Sprintf("%v", m[firstOutputName])
	} else {
		r.ExecReport.Result = fmt.Sprintf("%v", m)
	}
	// saving execution report for this function
	//compRequest.ExecReport.Reports[CreateExecutionReportId(s)] = &r.ExecReport
	compRequest.ExecReport.Reports.Set(CreateExecutionReportId(s), &r.ExecReport)
	/*
		cs := ""
		if !r.ExecReport.IsWarmStart {
			cs = fmt.Sprintf("- cold start: %v", !r.ExecReport.IsWarmStart)
		}
		fmt.Printf("Function Request %s - result of simple node %s: %v %s\n", r.ReqId, s.Id, r.ExecReport.Result, cs)
	*/
	return m, nil
}

// AddOutput connects the output of the SimpleNode to another DagNode
func (s *SimpleNode) AddOutput(dag *Dag, dagNode DagNodeId) error {
	s.OutputTo = dagNode
	return nil
}

func (s *SimpleNode) CheckInput(input map[string]interface{}) error {
	// TODO: must communicate and receive input from other node, if on another machine
	funct, exists := function.GetFunction(s.Func) // we are getting the function from cache if not already downloaded
	if !exists {
		return fmt.Errorf("funtion %s doesn't exists", s.Func)
	}

	if funct.Signature == nil {
		return nil // signature is optional, but if set, you can debug errors more easily
	}

	err := funct.Signature.CheckAllInputs(input)
	if err != nil {
		return fmt.Errorf("error while receiving input: %v", err)
	}
	//s.inputMutex.Lock()
	//s.input = input
	//s.inputMutex.Unlock()
	return nil
}

func (s *SimpleNode) PrepareOutput(dag *Dag, output map[string]interface{}) error {
	funct, exists := function.GetFunction(s.Func) // we are getting the function from cache if not already downloaded
	if !exists {
		return fmt.Errorf("funtion %s doesn't exists", s.Func)
	}
	err := funct.Signature.CheckAllOutputs(output)
	if err != nil {
		return fmt.Errorf("error while checking outputs: %v", err)
	}
	// get signature of next nodes, if present and maps the output there
	for _, n := range s.GetNext() {
		// we have only one output node
		dagNode, _ := dag.Find(n)
		switch nodeType := dagNode.(type) {
		case *SimpleNode:
			return nodeType.MapOutput(output) // needed to convert type of data from one node to the next so that its signature type-checks
		}
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

func (s *SimpleNode) GetNext() []DagNodeId {
	// we only have one output
	return []DagNodeId{s.OutputTo}
}

func (s *SimpleNode) Width() int {
	return 1
}
func (s *SimpleNode) Name() string {
	return "Simple"
}

func (s *SimpleNode) ToString() string {
	return fmt.Sprintf("[SimpleNode (%s) func %s()]->%s", s.Id, s.Func, s.OutputTo)
}

func (s *SimpleNode) setBranchId(number int) {
	s.BranchId = number
}
func (s *SimpleNode) GetBranchId() int {
	return s.BranchId
}

func (s *SimpleNode) GetId() DagNodeId {
	return s.Id
}

func (s *SimpleNode) GetNodeType() DagNodeType {
	return s.NodeType
}
