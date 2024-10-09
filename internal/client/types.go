package client

import (
	"github.com/grussorusso/serverledge/internal/function"
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

// CompositionInvocationRequest is an external invocation of a function composition (from API or CLI)
type CompositionInvocationRequest struct {
	Params          map[string]interface{}
	RequestQoSMap   map[string]function.RequestQoS
	QosMaxRespT     float64
	CanDoOffloading bool
	Async           bool
	// NextNodes       []string // DagNodeId
	// we do not add Progress here, only the next group of node that should execute
	// in case of choice node, we retrieve the progress for each dagNodeId and execute only the one that is not in Skipped State
	// in case of fan out node, we retrieve all the progress and execute concurrently all the dagNodes in the group.
	// in case of fan in node, we retrieve periodically all the progress of the previous nodes and start the merging only when all previous node are completed.
	//   or simply, we can get the N partialData for the Fan Out, coming from the previous nodes.
	//   furthermore, we should be careful not to run multiple fanIn at the same time!
}

type CompositionCreationFromASLRequest struct {
	Name               string // Name of the new composition
	RmFnOnDeletionFlag bool   // flag to allow function remove on deletion of the FC
	ASLSrc             string // Specification source in Amazon State Language (encoded in Base64)
}
