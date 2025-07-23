package workflow

import (
	"github.com/serverledge-faas/serverledge/internal/client"
	"time"

	"github.com/serverledge-faas/serverledge/internal/function"
)

type ReqId string

// Request represents a workflow invocation, with params and metrics data
type Request struct {
	Id              string
	W               *Workflow
	Params          map[string]interface{}
	Arrival         time.Time
	ExecReport      ExecutionReport     // each function has its execution report, and the workflow has additional metrics
	QoS             function.RequestQoS // every function should have its QoS
	CanDoOffloading bool                // every function inherits this flag
	Async           bool
	Resuming        bool            // indicating whether the function is resuming from a previous (partial) execution
	Plan            *OffloadingPlan // optional; execution plan
}

func NewRequest(reqId string, workflow *Workflow, params map[string]interface{}) *Request {
	return &Request{
		Id:      reqId,
		W:       workflow,
		Params:  params,
		Arrival: time.Now(),
		ExecReport: ExecutionReport{
			Reports: map[string]*function.ExecutionReport{},
		},
		CanDoOffloading: true,
		Async:           false,
		Resuming:        false,
	}
}

type InvocationResponse struct {
	Success      bool
	Result       map[string]interface{}
	Reports      map[string]*function.ExecutionReport
	ResponseTime float64 // time waited by the user to get the output of the entire workflow (in seconds)
}

type AsyncInvocationResponse struct {
	ReqId string
}

// WorkflowInvocationResumeRequest is a request to resume the execution of a workflow (typically on a remote node)
type WorkflowInvocationResumeRequest struct {
	ReqId string
	client.WorkflowInvocationRequest
	Plan OffloadingPlan
}
