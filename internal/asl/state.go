package asl

import (
	"fmt"

	"github.com/serverledge-faas/serverledge/internal/types"
)

// State is the common interface for ASL states. StateTypes are Task, Parallel, Map, Pass, Wait, Choice, Succeed, Fail
type State interface {
	fmt.Stringer
	types.Comparable
	Validatable
	GetType() StateType
}

// StateType for ASL states
type StateType string

const (
	Task     StateType = "Task"
	Parallel StateType = "Parallel"
	Map      StateType = "Map"
	Pass     StateType = "Pass"
	Wait     StateType = "Wait"
	Choice   StateType = "Choice"
	Succeed  StateType = "Succeed"
	Fail     StateType = "Fail"
)

// InputPath is a field that indicates from where to read input
type InputPath = string

// OutputPath is a field that indicates where to write output
type OutputPath = string

// HasNext is an interface for non-terminal states.
// Fail, Succeed and any other state with End=true are terminal states.
// Fail and Succeed should not implement this interface
// Should be implemented by Task, Parallel,	Map, Pass and Wait
type HasNext interface {
	// GetNext returns the next state, if exists. Otherwise, returns an empty string and false
	GetNext() (string, bool)
}

// CanEnd is an interface with a boolean method. If true, marks the state machine end. Valid for Task, Parallel, Map, Pass and Wait states
type CanEnd interface {
	IsEndState() bool
}

// ResultPath is a string field that indicates the result path
type ResultPath = string

// Parameters is a JSON string payload template with names and values of input parameters for Task, Map and Parallel states
type Parameters = string
type HasParameters interface {
	GetParameters() Parameters
}

// ResultSelector is a reference path that indicates where to place the output relative to the raw input.
type ResultSelector = string // TODO: what is that

type HasResources interface {
	// GetResources returns all function names present in the State. The implementation could return duplicate functions
	GetResources() []string
}

type Parsable interface {
	fmt.Stringer
	ParseFrom(jsonData []byte) (State, error)
}

// Validatable checks every state of the state machine. Use it when the state machine is complete
type Validatable interface {
	Validate(stateNames []string) error
}
