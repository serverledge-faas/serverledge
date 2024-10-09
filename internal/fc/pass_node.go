package fc

import (
	"fmt"
	"time"

	"github.com/grussorusso/serverledge/internal/function"
	"github.com/grussorusso/serverledge/internal/types"
	"github.com/lithammer/shortuuid"
)

type PassNode struct {
	Id         DagNodeId
	NodeType   DagNodeType
	Result     string
	ResultPath string
	OutputTo   DagNodeId
	BranchId   int
}

func NewPassNode(result string) *PassNode {
	passNode := PassNode{
		Id:       DagNodeId("pass_" + shortuuid.New()),
		NodeType: Pass,
		Result:   result,
	}
	return &passNode
}

func (p *PassNode) Exec(compRequest *CompositionRequest, params ...map[string]interface{}) (map[string]interface{}, error) {
	t0 := time.Now()
	var err error = nil
	if len(params) != 1 {
		return nil, fmt.Errorf("failed to get one input for pass node: received %d inputs", len(params))
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
	compRequest.ExecReport.Reports.Set(CreateExecutionReportId(p), execReport)
	return output, err
}

func (p *PassNode) Equals(cmp types.Comparable) bool {
	p2, ok := cmp.(*PassNode)
	if !ok {
		return false
	}
	return p.Id == p2.Id &&
		p.NodeType == p2.NodeType &&
		p.Result == p2.Result &&
		p.ResultPath == p2.ResultPath &&
		p.OutputTo == p2.OutputTo &&
		p.BranchId == p2.BranchId
}

func (p *PassNode) CheckInput(input map[string]interface{}) error {
	return nil
}

// AddOutput for a PassNode connects it to another DagNode, except StartNode
func (p *PassNode) AddOutput(dag *Dag, dagNode DagNodeId) error {
	_, ok := dag.Nodes[dagNode].(*StartNode)
	if ok {
		return fmt.Errorf("the PassNode cannot be chained to a startNode")
	}
	p.OutputTo = dagNode
	return nil
}

func (p *PassNode) PrepareOutput(dag *Dag, output map[string]interface{}) error {
	if p.ResultPath != "" {
		return fmt.Errorf("ResultPath not currently implemented") // TODO: implement it
	}

	if len(p.GetNext()) == 0 {
		return fmt.Errorf("failed to map output: there are no next node after PassNode")
	}
	// Get the next node.
	nextNodeId := p.GetNext()[0]

	nextNode, ok := dag.Find(nextNodeId)
	if !ok {
		return fmt.Errorf("failed to find next node")
	}

	// If it is a SimpleNode
	nextSimpleNode, ok := nextNode.(*SimpleNode)
	if !ok {
		return nil
	}
	return p.MapOutput(nextSimpleNode, output)
}

// MapOutput changes the names of the output parameters to match the name of the input parameters of the next SimpleNode
func (p *PassNode) MapOutput(nextNode *SimpleNode, output map[string]interface{}) error {
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

func (p *PassNode) GetNext() []DagNodeId {
	return []DagNodeId{p.OutputTo}
}

func (p *PassNode) Width() int {
	return 1
}

func (p *PassNode) Name() string {
	return "Pass"
}

func (p *PassNode) String() string {
	return "[ Pass ]"
}

func (p *PassNode) setBranchId(number int) {
	p.BranchId = number
}

func (p *PassNode) GetBranchId() int {
	return p.BranchId
}

func (p *PassNode) GetId() DagNodeId {
	return p.Id
}

func (p *PassNode) GetNodeType() DagNodeType {
	return p.NodeType
}
