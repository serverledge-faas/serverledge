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
	// NextNodes       []string // DagNodeId
	// we do not add Progress here, only the next group of node that should execute
	// in case of choice node, we retrieve the progress for each taskId and execute only the one that is not in Skipped State
	// in case of fan out node, we retrieve all the progress and execute concurrently all the tasks in the group.
	// in case of fan in node, we retrieve periodically all the progress of the previous nodes and start the merging only when all previous node are completed.
	//   or simply, we can get the N partialData for the Fan Out, coming from the previous nodes.
	//   furthermore, we should be careful not to run multiple fanIn at the same time!
}

type WorkflowCreationRequest struct {
	Name   string // Name of the new workflow
	ASLSrc string // Specification source in Amazon State Language (encoded in Base64)
}
