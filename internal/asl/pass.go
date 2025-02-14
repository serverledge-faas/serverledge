package asl

import "github.com/serverledge-faas/serverledge/internal/types"

type PassState struct {
	Type StateType
	Next string
	End  bool
}

func (p *PassState) Validate(stateNames []string) error {
	//TODO implement me
	panic("implement me")
}

func (p *PassState) IsEndState() bool {
	return p.End
}

func (p *PassState) Equals(cmp types.Comparable) bool {
	p2 := cmp.(*PassState)
	return p.Type == p2.Type && p.Next == p2.Next && p.End == p2.End
}

func NewEmptyPass() *PassState {
	return &PassState{
		Type: Pass,
		Next: "",
		End:  false,
	}
}

func (p *PassState) ParseFrom(jsonData []byte) (State, error) {
	//TODO implement me
	panic("implement me")
}

func (p *PassState) GetNext() (string, bool) {
	if p.End == false {
		return p.Next, true
	}
	return "", false
}

func (p *PassState) GetType() StateType {
	return Pass
}

func (p *PassState) String() string {
	//TODO implement me
	panic("implement me")
}
