package workflow

import (
	"time"

	"github.com/cornelk/hashmap"
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
}

func NewRequest(reqId string, workflow *Workflow, params map[string]interface{}) *Request {
	return &Request{
		Id:      reqId,
		W:       workflow,
		Params:  params,
		Arrival: time.Now(),
		ExecReport: ExecutionReport{
			Reports: hashmap.New[ExecutionReportId, *function.ExecutionReport](), // make(map[ExecutionReportId]*function.ExecutionReport),
		},
		CanDoOffloading: true,
		Async:           false,
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
