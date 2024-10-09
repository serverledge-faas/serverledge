package fc

import (
	"fmt"
	"time"

	"github.com/grussorusso/serverledge/internal/function"
	"github.com/grussorusso/serverledge/internal/types"
	"github.com/lithammer/shortuuid"
)

type WaitNode struct {
	Id        DagNodeId
	NodeType  DagNodeType
	Seconds   int
	Timestamp *time.Time
	OutputTo  DagNodeId
	BranchId  int
}

func NewWaitNode(durationSeconds int) *WaitNode {
	if durationSeconds < 0 {
		durationSeconds = 0
	}
	waitNode := WaitNode{
		Id:       DagNodeId("wait_" + shortuuid.New()),
		NodeType: Wait,
		Seconds:  durationSeconds,
	}
	return &waitNode
}

func NewWaitNodeFromTimestamp(timestamp time.Time) *WaitNode {
	if timestamp.Before(time.Now()) {
		timestamp = time.Now()
	}
	waitNode := WaitNode{
		Id:        DagNodeId("wait_" + shortuuid.New()),
		NodeType:  Wait,
		Timestamp: &timestamp,
	}
	return &waitNode
}

func (w *WaitNode) Exec(compRequest *CompositionRequest, params ...map[string]interface{}) (map[string]interface{}, error) {
	t0 := time.Now()
	var err error = nil
	if len(params) != 1 {
		return nil, fmt.Errorf("failed to get one input for wait node: received %d inputs", len(params))
	}

	// wait until timestamp is reached or for the specified number of seconds
	if w.Timestamp != nil {
		duration := time.Until(*w.Timestamp)
		if duration > 0 {
			time.Sleep(duration)
		}
	} else {
		time.Sleep(time.Duration(w.Seconds) * time.Second)
	}

	output := params[0]
	respAndDuration := time.Now().Sub(t0).Seconds()
	execReport := &function.ExecutionReport{
		Result:         fmt.Sprintf("%v", output),
		ResponseTime:   respAndDuration,
		IsWarmStart:    true, // not in a container
		InitTime:       0,
		OffloadLatency: 0,
		Duration:       respAndDuration,
		SchedAction:    "",
	}
	compRequest.ExecReport.Reports.Set(CreateExecutionReportId(w), execReport)
	return output, err
}

func (w *WaitNode) Equals(cmp types.Comparable) bool {
	w2, ok := cmp.(*WaitNode)
	if !ok {
		return false
	}
	return w.Id == w2.Id &&
		w.NodeType == w2.NodeType &&
		w.Timestamp == w2.Timestamp &&
		w.Seconds == w2.Seconds &&
		w.OutputTo == w2.OutputTo &&
		w.BranchId == w2.BranchId
}

func (w *WaitNode) CheckInput(input map[string]interface{}) error {
	return nil
}

func (w *WaitNode) AddOutput(dag *Dag, dagNode DagNodeId) error {
	_, ok := dag.Nodes[dagNode].(*StartNode)
	if ok {
		return fmt.Errorf("the WaitNode cannot be chained to a startNode")
	}
	w.OutputTo = dagNode
	return nil
}

func (w *WaitNode) PrepareOutput(dag *Dag, output map[string]interface{}) error {
	if len(w.GetNext()) == 0 {
		return fmt.Errorf("failed to map output: there are no next node after PassNode")
	}
	// Get the next node.
	nextNodeId := w.GetNext()[0]

	nextNode, ok := dag.Find(nextNodeId)
	if !ok {
		return fmt.Errorf("failed to find next node")
	}

	// If it is a SimpleNode
	nextSimpleNode, ok := nextNode.(*SimpleNode)
	if !ok {
		return nil
	}
	return w.MapOutput(nextSimpleNode, output)
}

// MapOutput changes the names of the output parameters to match the name of the input parameters of the next SimpleNode
func (w *WaitNode) MapOutput(nextNode *SimpleNode, output map[string]interface{}) error {
	funct, exists := function.GetFunction(nextNode.Func)
	if !exists {
		return fmt.Errorf("function %s doesn't exist", nextNode.Func)
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

func (w *WaitNode) GetNext() []DagNodeId {
	return []DagNodeId{w.OutputTo}
}

func (w *WaitNode) Width() int {
	return 1
}

func (w *WaitNode) Name() string {
	return "Wait"
}

func (w *WaitNode) String() string {
	return "[ Wait ]"
}

func (w *WaitNode) setBranchId(number int) {
	w.BranchId = number
}

func (w *WaitNode) GetBranchId() int {
	return w.BranchId
}

func (w *WaitNode) GetId() DagNodeId {
	return w.Id
}

func (w *WaitNode) GetNodeType() DagNodeType {
	return w.NodeType
}
