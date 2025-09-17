package workflow

type OffloadingDecision struct {
	Offload    bool   `json:"offload"`
	RemoteHost string `json:"remote_host"`
	OffloadingPlan
}

type OffloadingPolicy interface {
	Init()
	Evaluate(r *Request, p *Progress) (OffloadingDecision, error)
}

type OffloadingPlan struct {
	ToExecute []TaskId
}

type NoOffloadingPolicy struct{}

func (policy *NoOffloadingPolicy) Init() {
}

func (policy *NoOffloadingPolicy) Evaluate(r *Request, p *Progress) (OffloadingDecision, error) {

	return OffloadingDecision{Offload: false}, nil
}
