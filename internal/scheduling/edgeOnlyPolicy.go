package scheduling

import (
	"github.com/serverledge-faas/serverledge/internal/function"
	"github.com/serverledge-faas/serverledge/internal/node"
)

// EdgePolicy supports only Edge-Edge offloading. Always does offloading to an edge node if enabled. When offloading is not enabled executes the request locally.
type EdgePolicy struct{}

func (p *EdgePolicy) Init() {
}

func (p *EdgePolicy) OnCompletion(_ *function.Function, _ *function.ExecutionReport) {

}

func (p *EdgePolicy) OnArrival(r *scheduledRequest) {
	if r.CanDoOffloading {
		url := pickEdgeNodeForOffloading(r)
		if url != "" {
			handleOffload(r, url)
			return
		}
	} else {
		containerID, warm, err := node.AcquireContainer(r.Fun)
		if err == nil {
			execLocally(r, containerID, warm)
			return
		}
	}

	dropRequest(r)
}
