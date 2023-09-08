package scheduling

import (
	"github.com/serverledge-faas/serverledge/internal/function"
	"log"

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
		containerID, err := node.AcquireWarmContainer(r.Fun)
		if err == nil {
			log.Printf("Using a warm container for: %v\n", r)
			execLocally(r, containerID, true)
		} else if handleColdStart(r) {
			return
		}
	}

	dropRequest(r)
}
