package scheduling

import (
	"log"

	"github.com/serverledge-faas/serverledge/internal/function"
	"github.com/serverledge-faas/serverledge/internal/node"
)

// CloudEdgePolicy supports only Edge-Cloud Offloading. Executes locally first,
// but if no resources are available and offload is enabled offloads the request to a cloud node.
// If no resources are available and offloading is disabled, drops the request.
type CloudEdgePolicy struct{}

func (p *CloudEdgePolicy) Init() {
}

func (p *CloudEdgePolicy) OnCompletion(_ *function.Function, _ *function.ExecutionReport) {

}

func (p *CloudEdgePolicy) OnArrival(r *scheduledRequest) {

	canRunLocally := r.Fun.SupportsArch(node.LocalNode.Arch)
	if canRunLocally {
		containerID, warm, err := node.AcquireContainer(r.Fun, false)
		if err == nil {
			execLocally(r, containerID, warm)
			return
		}
	}

	if r.CanDoOffloading {
		handleCloudOffload(r)
	} else {
		log.Printf("Dropping request because cannot exec locally (architecutre is supported: %t) and cannot offload", canRunLocally)
		dropRequest(r)
	}
}
