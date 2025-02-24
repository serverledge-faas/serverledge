package fc

import (
	"fmt"
	"time"

	"github.com/grussorusso/serverledge/internal/function"
	"github.com/grussorusso/serverledge/internal/types"
	"github.com/lithammer/shortuuid"
)

type FailNode struct {
	Id       TaskId
	NodeType TaskType
	Error    string
	Cause    string

	/* (Serverledge specific) */

	// OutputTo for a SucceedNode is used to send the output to the EndNode
	OutputTo TaskId
	BranchId int
}

func (f *FailNode) PrepareOutput(workflow *Workflow, output map[string]interface{}) error {
	output[f.Error] = f.Cause
	return nil
}

func NewFailNode(error, cause string) *FailNode {
	if len(error) > 20 {
		fmt.Printf("error string identifier should be less than 20 characters but is %d characters long\n", len(error))
	}
	fail := FailNode{
		Id:       TaskId("fail_" + shortuuid.New()),
		NodeType: Fail,
		Error:    error,
		Cause:    cause,
	}
	return &fail
}

func (f *FailNode) Exec(compRequest *CompositionRequest, params ...map[string]interface{}) (map[string]interface{}, error) {
	t0 := time.Now()
	output := make(map[string]interface{})
	var err error = nil
	if len(params) != 1 {
		return nil, fmt.Errorf("failed to get one input for fail node: received %d inputs", len(params))
	}
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
	compRequest.ExecReport.Reports.Set(CreateExecutionReportId(f), execReport)
	return output, err
}

func (f *FailNode) Equals(cmp types.Comparable) bool {
	f2, ok := cmp.(*FailNode)
	if !ok {
		return false
	}
	return f.Id == f2.Id &&
		f.NodeType == f2.NodeType &&
		f.Error == f2.Error &&
		f.Cause == f2.Cause &&
		f.OutputTo == f2.OutputTo &&
		f.BranchId == f2.BranchId
}

func (f *FailNode) CheckInput(input map[string]interface{}) error {
	return nil
}

func (f *FailNode) AddOutput(workflow *Workflow, taskId TaskId) error {
	_, ok := workflow.Nodes[taskId].(*EndNode)
	if !ok {
		return fmt.Errorf("the FailNode can only be chained to an end node")
	}
	f.OutputTo = taskId
	return nil
}

//func (f *FailNode) PrepareOutput(workflow *Workflow, output map[string]interface{}) error {
//	return nil
//}

func (f *FailNode) GetNext() []TaskId {
	return []TaskId{f.OutputTo}
}

func (f *FailNode) Width() int {
	return 1
}

func (f *FailNode) Name() string {
	return " Fail "
}

func (f *FailNode) String() string {
	return fmt.Sprintf("[Fail: %s]", f.Error)
}

func (f *FailNode) GetId() TaskId {
	return f.Id
}

func (f *FailNode) setBranchId(number int) {
	f.BranchId = number
}

func (f *FailNode) GetBranchId() int {
	return f.BranchId
}

func (f *FailNode) GetNodeType() TaskType {
	return f.NodeType
}
