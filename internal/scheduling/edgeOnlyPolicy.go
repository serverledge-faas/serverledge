package scheduling

import (
	"errors"
	"log"

	"github.com/serverledge-faas/serverledge/internal/config"
	"github.com/serverledge-faas/serverledge/internal/function"
	"github.com/serverledge-faas/serverledge/internal/node"
)

// EdgePolicy supports only Edge-Edge offloading. Always does offloading to an edge node if enabled. When offloading is not enabled executes the request locally.
type EdgePolicy struct{}

var fallBackLocally bool

func (p *EdgePolicy) Init() {
	fallBackLocally = config.GetBool(config.SCHEDULING_FALLBACK_LOCAL, false)
	log.Printf("[INFO] Initializing EdgePolicy. Fallback to local execution set to: %t\n", fallBackLocally)
}

func (p *EdgePolicy) OnCompletion(_ *function.Function, _ *function.ExecutionReport) {

}

func (p *EdgePolicy) OnArrival(r *scheduledRequest) {

	if r.CanDoOffloading {
		url, err := pickEdgeNodeForOffloading(r) // this will now take into account the node architecture in the offloading process
		if url != "" {
			handleOffload(r, url)
			return
		} else if errors.Is(err, NoSuitableNode) && fallBackLocally {
			// This is the case where offloading could've been possible (I had available neighbors)
			// but they ALL were of a mismatching architecture.
			// E.g.: r.Fun.SupportedArchs = {"amd64"}, but all nNeighbors are arm-based.

			tryLocalExecution(r)
		}
	} else {
		tryLocalExecution(r)
	}
	dropRequest(r) // r.CanDoOffloading == true, NoSuitableNode == true && fallBackLocally == false leads here, so we drop
	// the request in that case
}

func tryLocalExecution(r *scheduledRequest) {
	if !r.Fun.SupportsArch(node.LocalNode.Arch) {
		// If the current node architecture is not supported by the function's runtime, we can only drop it, since
		// offloading was already tried unsuccessfully, or it was disabled for this request.
		dropRequest(r)
		return

	}

	containerID, warm, err := node.AcquireContainer(r.Fun, false)
	if err == nil {
		execLocally(r, containerID, warm)
		return
	}
}
