package fc

import (
	"github.com/grussorusso/serverledge/internal/types"
)

type DagNodeId string

// DagNode is an interface for a single node in the Dag
// all implementors must be pointers to a struct
type DagNode interface {
	types.Comparable
	Display
	Executable
	HasOutput
	ChecksInput
	ReceivesOutput
	HasNext
	Width() int
	HasBranch
	HasNodeType
}

// HasBranch is a counter that represent the branch of a node in the dag.
// For a sequence dag, the branch is always 0.
// For a dag with a single choice node, the choice node has branch 0, the N alternatives have branch 1,2,...,N
// For a parallel dag with one fanOut and fanIn, the fanOut has branch 0, fanOut branches have branch 1,2,...,N and FanIn has branch N+1
type HasBranch interface {
	setBranchId(number int)
	GetBranchId() int
}

type Display interface {
	ToString() string
	GetId() DagNodeId
	Name() string
}

type Executable interface {
	// Exec defines how the DagNode is executed. If successful, returns the output of the execution
	Exec(compRequest *CompositionRequest, params ...map[string]interface{}) (map[string]interface{}, error)
}

type HasOutput interface {
	// AddOutput  adds a result node, if compatible. For some DagNodes can be called multiple times
	AddOutput(dag *Dag, dagNode DagNodeId) error
}

type ChecksInput interface {
	// CheckInput checks the input and if necessary tries to convert into a suitable representation for the executing function
	CheckInput(input map[string]interface{}) error
}

type ReceivesOutput interface {
	// PrepareOutput maps the outputMap of the current node to the inputMap of the next nodes
	PrepareOutput(dag *Dag, output map[string]interface{}) error
}

type HasNext interface {
	GetNext() []DagNodeId
}

type HasNodeType interface {
	GetNodeType() DagNodeType
}

func Equals[D DagNode](d1 D, d2 D) bool {
	return d1.Equals(d2)
}
