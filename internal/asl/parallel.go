package asl

import "github.com/serverledge-faas/serverledge/internal/types"

type ParallelState struct {
	Type     StateType
	Branches []*StateMachine
	Next     string
	End      bool
}

func (p *ParallelState) Validate(stateNames []string) error {
	//TODO implement me
	panic("implement me")
}

func (p *ParallelState) IsEndState() bool {
	return p.End
}

func (p *ParallelState) GetResources() []string {
	funcs := make([]string, 0)
	for _, branchStateMachine := range p.Branches {
		funcs = append(funcs, branchStateMachine.GetFunctionNames()...)
	}
	return funcs
}

func (p *ParallelState) Equals(cmp types.Comparable) bool {
	p2 := cmp.(*ParallelState)
	return p.Type == p2.Type
}

func NewEmptyParallel() *ParallelState {
	return &ParallelState{
		Type:     Parallel,
		Branches: make([]*StateMachine, 0),
		Next:     "",
		End:      false,
	}
}

func (p *ParallelState) ParseFrom(jsonData []byte) (State, error) {
	//TODO implement me
	panic("implement me")
}

func (p *ParallelState) GetNext() (string, bool) {
	if p.End == false {
		return p.Next, true
	}
	return "", false
}

func (p *ParallelState) GetType() StateType {
	return Parallel
}

// FIXME: improve
func (p *ParallelState) String() string {
	return "Parallel"
}
