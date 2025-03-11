package workflow

import (
	"fmt"
	"github.com/grussorusso/serverledge/internal/types"
	"github.com/lithammer/shortuuid"
)

type PassNode struct {
	Id         TaskId
	NodeType   TaskType
	Result     string
	ResultPath string
	OutputTo   TaskId
	BranchId   int
}

func NewPassNode(result string) *PassNode {
	passNode := PassNode{
		Id:       TaskId("pass_" + shortuuid.New()),
		NodeType: Pass,
		Result:   result,
	}
	return &passNode
}

func (p *PassNode) Exec(compRequest *Request, params ...map[string]interface{}) (map[string]interface{}, error) {
	return nil, nil
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

// AddOutput for a PassNode connects it to another Task, except StartNode
func (p *PassNode) AddOutput(workflow *Workflow, taskId TaskId) error {
	_, ok := workflow.Nodes[taskId].(*StartNode)
	if ok {
		return fmt.Errorf("the PassNode cannot be chained to a startNode")
	}
	p.OutputTo = taskId
	return nil
}

func (p *PassNode) PrepareOutput(workflow *Workflow, output map[string]interface{}) error {
	return nil
}

func (p *PassNode) GetNext() []TaskId {
	return []TaskId{p.OutputTo}
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

func (p *PassNode) GetId() TaskId {
	return p.Id
}

func (p *PassNode) GetNodeType() TaskType {
	return p.NodeType
}
