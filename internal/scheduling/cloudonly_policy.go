package scheduling

// CloudOnlyPolicy can be used on Edge nodes to always offload on cloud. If offloading is disabled, the request is dropped
import "github.com/serverledge-faas/serverledge/internal/function"

type CloudOnlyPolicy struct{}

func (p *CloudOnlyPolicy) Init() {
}

func (p *CloudOnlyPolicy) OnCompletion(_ *function.Function, _ *function.ExecutionReport) {

}

func (p *CloudOnlyPolicy) OnArrival(r *scheduledRequest) {
	if r.CanDoOffloading {
		handleCloudOffload(r)
	} else {
		dropRequest(r)
	}
}
