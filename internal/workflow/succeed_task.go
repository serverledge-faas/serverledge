package workflow

import (
	"fmt"
	"time"

	"github.com/grussorusso/serverledge/internal/function"
	"github.com/grussorusso/serverledge/internal/types"
	"github.com/lithammer/shortuuid"
)

type SucceedNode struct {
	Id         TaskId
	NodeType   TaskType
	InputPath  string
	OutputPath string

	/* (Serverledge specific) */
	Message string
	// OutputTo for a SucceedNode is used to send the output to the EndNode
	OutputTo TaskId
	BranchId int
}

func NewSucceedNode(message string) *SucceedNode {
	succeedNode := SucceedNode{
		Id:       TaskId("succeed_" + shortuuid.New()),
		NodeType: Succeed,
		Message:  message,
	}
	return &succeedNode
}

func (s *SucceedNode) Exec(compRequest *Request, params ...map[string]interface{}) (map[string]interface{}, error) {
	t0 := time.Now()
	var err error = nil
	if len(params) != 1 {
		return nil, fmt.Errorf("failed to get one input for succeed node: received %d inputs", len(params))
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
	compRequest.ExecReport.Reports.Set(CreateExecutionReportId(s), execReport)
	return output, err
}

func (s *SucceedNode) Equals(cmp types.Comparable) bool {
	s2, ok := cmp.(*SucceedNode)
	if !ok {
		return false
	}
	return s.Id == s2.Id &&
		s.NodeType == s2.NodeType &&
		s.InputPath == s2.InputPath &&
		s.OutputPath == s2.OutputPath &&
		s.OutputTo == s2.OutputTo &&
		s.BranchId == s2.BranchId &&
		s.Message == s2.Message
}

func (s *SucceedNode) CheckInput(input map[string]interface{}) error {
	return nil
}

func (s *SucceedNode) AddOutput(workflow *Workflow, taskId TaskId) error {
	_, ok := workflow.Nodes[taskId].(*EndNode)
	if !ok {
		return fmt.Errorf("the SucceedNode can only be chained to an end node")
	}
	s.OutputTo = taskId
	return nil
}

// PrepareOutput can be used in a SucceedNode to modify the workflow output representation
func (s *SucceedNode) PrepareOutput(workflow *Workflow, output map[string]interface{}) error {
	if s.OutputPath != "" {
		return fmt.Errorf("OutputPath not currently implemented") // TODO: implement it
	}
	return nil
}

func (s *SucceedNode) GetNext() []TaskId {
	return []TaskId{s.OutputTo}
}

func (s *SucceedNode) Width() int {
	return 1
}

func (s *SucceedNode) Name() string {
	return "Success"
}

func (s *SucceedNode) String() string {
	return "[Succeed]"
}

func (s *SucceedNode) setBranchId(number int) {
	s.BranchId = number
}

func (s *SucceedNode) GetBranchId() int {
	return s.BranchId
}

func (s *SucceedNode) GetId() TaskId {
	return s.Id
}

func (s *SucceedNode) GetNodeType() TaskType {
	return s.NodeType
}
