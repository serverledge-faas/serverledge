package client

import (
	"github.com/serverledge-faas/serverledge/internal/function"
)

// InvocationRequest is an external invocation of a function (from API or CLI)
type InvocationRequest struct {
	Params          map[string]interface{}
	QoSClass        function.ServiceClass
	QoSMaxRespT     float64
	CanDoOffloading bool
	Async           bool
	ReturnOutput    bool
}

type PrewarmingRequest struct {
	Function       string
	Instances      int64
	ForceImagePull bool
}

// WorkflowInvocationRequest is an external invocation of a workflow (from API or CLI)
type WorkflowInvocationRequest struct {
	Params          map[string]interface{}
	QoS             function.RequestQoS
	CanDoOffloading bool
	Async           bool
}

// WorkflowInvocationResumeRequest is a request to resume the execution of a workflow (typically on a remote node)
type WorkflowInvocationResumeRequest struct {
	ReqId string
	WorkflowInvocationRequest
}

type WorkflowCreationRequest struct {
	Name   string // Name of the new workflow
	ASLSrc string // Specification source in Amazon State Language (encoded in Base64)
}
