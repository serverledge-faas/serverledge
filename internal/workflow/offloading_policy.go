package workflow

type OffloadingDecision struct {
	Offload    bool   `json:"offload"`
	RemoteHost string `json:"remote_host"`
}

type OffloadingPolicy interface {
	Evaluate(r *Request, p *Progress) (OffloadingDecision, error)
}

type NoOffloadingPolicy struct{}

func (policy *NoOffloadingPolicy) Evaluate(r *Request, p *Progress) (OffloadingDecision, error) {

	return OffloadingDecision{Offload: false}, nil
}

type SimpleOffloadingPolicy struct{}

func (policy *SimpleOffloadingPolicy) Evaluate(r *Request, p *Progress) (OffloadingDecision, error) {

	completed := 0

	if p == nil || !r.CanDoOffloading || len(p.ReadyToExecute) == 0 {
		return OffloadingDecision{Offload: false}, nil
	}

	for _, s := range p.Status {
		if s == Executed {
			completed++
		}
	}

	if completed >= 2 {
		return OffloadingDecision{true, "127.0.0.1:1323"}, nil
	}

	return OffloadingDecision{Offload: false}, nil
}
