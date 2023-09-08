package scheduling

import (
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
	containerID, err := node.AcquireWarmContainer(r.Fun)
	if err == nil {
		execLocally(r, containerID, true)
	} else if handleColdStart(r) {
		return
	} else if r.CanDoOffloading {
		handleCloudOffload(r)
	} else {
		dropRequest(r)
	}
}
