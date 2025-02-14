package asl

import (
	"fmt"

	"github.com/serverledge-faas/serverledge/internal/types"
)

type SucceedState struct {
	Type       StateType // Necessary
	InputPath  Path      // Optional, default $
	OutputPath Path      // Optional, default $
}

func (s *SucceedState) Validate(stateNames []string) error {
	return nil
}

func (s *SucceedState) IsEndState() bool {
	return true
}

func (s *SucceedState) Equals(cmp types.Comparable) bool {
	s2 := cmp.(*SucceedState)
	return s.Type == s2.Type &&
		s.InputPath == s2.InputPath &&
		s.OutputPath == s2.OutputPath
}

func NewEmptySucceed() *SucceedState {
	return &SucceedState{
		Type: Succeed,
	}
}

func (s *SucceedState) ParseFrom(jsonData []byte) (State, error) {
	s.InputPath = JsonExtractRefPathOrDefault(jsonData, "InputPath", "")
	s.OutputPath = JsonExtractRefPathOrDefault(jsonData, "OutputPath", "")
	return s, nil
}

func (s *SucceedState) GetType() StateType {
	return Succeed
}

func (s *SucceedState) String() string {
	str := fmt.Sprint("{",
		"\n\t\t\tType: ", s.Type,
		"\n")
	if s.InputPath != "" {
		str += fmt.Sprintf("\t\t\tError: %s\n", s.InputPath)
	}
	if s.OutputPath != "" {
		str += fmt.Sprintf("\t\t\tErrorPath: %s\n", s.OutputPath)
	}
	str += "\t\t}"
	return str
}
