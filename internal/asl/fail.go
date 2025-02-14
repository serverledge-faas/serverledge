package asl

import (
	"fmt"

	"github.com/serverledge-faas/serverledge/internal/types"
)

type FailState struct {
	Type StateType
	// Error is a error identifier to be used in a retry State
	Error     string
	ErrorPath Path
	// Cause is a human-readable message
	Cause     string
	CausePath Path
}

func (f *FailState) Validate(stateNames []string) error {
	if f.Error != "" && f.ErrorPath != "" {
		return fmt.Errorf("the Error and ErrorPath fields cannot be both set at the same time")
	}

	if f.Cause != "" && f.CausePath != "" {
		return fmt.Errorf("the Cause and CausePath fields cannot be both set at the same time")
	}
	return nil
}

func (f *FailState) IsEndState() bool {
	return true
}

func (f *FailState) Equals(cmp types.Comparable) bool {
	f2 := cmp.(*FailState)
	return f.Type == f2.Type &&
		f.Error == f2.Error &&
		f.ErrorPath == f2.ErrorPath &&
		f.Cause == f2.Cause &&
		f.CausePath == f2.CausePath
}

func NewEmptyFail() *FailState {
	return &FailState{
		Type: Fail,
	}
}

func (f *FailState) ParseFrom(jsonData []byte) (State, error) {
	f.Error = JsonExtractStringOrDefault(jsonData, "Error", "")
	f.ErrorPath = JsonExtractRefPathOrDefault(jsonData, "ErrorPath", "")
	f.Cause = JsonExtractStringOrDefault(jsonData, "Cause", "")
	f.CausePath = JsonExtractRefPathOrDefault(jsonData, "CausePath", "")
	return f, nil
}

func (f *FailState) GetType() StateType {
	return Fail
}

func (f *FailState) String() string {
	str := fmt.Sprint("{",
		"\n\t\t\tType: ", f.Type,
		"\n")
	if f.Error != "" {
		str += fmt.Sprintf("\t\t\tError: %s\n", f.Error)
	}
	if f.ErrorPath != "" {
		str += fmt.Sprintf("\t\t\tErrorPath: %s\n", f.ErrorPath)
	}
	if f.Cause != "" {
		str += fmt.Sprintf("\t\t\tCause: %s\n", f.Cause)
	}
	if f.CausePath != "" {
		str += fmt.Sprintf("\t\t\tCausePath: %s\n", f.CausePath)
	}
	str += "\t\t}"
	return str
}

func (f *FailState) GetError() string {
	if f.Error != "" {
		return f.Error
	} else if f.ErrorPath != "" {
		return string(f.ErrorPath) // will be evaluated at run time
	} else {
		return "GenericError"
	}
}

func (f *FailState) GetCause() string {
	if f.Cause != "" {
		return f.Cause
	} else if f.CausePath != "" {
		return string(f.CausePath) // will be evaluated at run time
	} else {
		return "Execution failed due to a generic error"
	}
}
