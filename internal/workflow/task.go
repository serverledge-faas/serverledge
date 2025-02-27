package workflow

import (
	"fmt"

	"github.com/grussorusso/serverledge/internal/types"
)

type TaskId string

// Task is an interface for a single node in the Workflow
// all implementors must be pointers to a struct
type Task interface {
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

// HasBranch is a counter that represent the branch of a node in the workflow.
// For a sequence workflow, the branch is always 0.
// For a workflow with a single choice node, the choice node has branch 0, the N alternatives have branch 1,2,...,N
// For a parallel workflow with one fanOut and fanIn, the fanOut has branch 0, fanOut branches have branch 1,2,...,N and FanIn has branch N+1
type HasBranch interface {
	setBranchId(number int)
	GetBranchId() int
}

type Display interface {
	fmt.Stringer
	GetId() TaskId
	Name() string
}

type Executable interface {
	// Exec defines how the Task is executed. If successful, returns the output of the execution
	Exec(compRequest *Request, params ...map[string]interface{}) (map[string]interface{}, error)
}

type HasOutput interface {
	// AddOutput  adds a result task, if compatible. For some task can be called multiple times
	AddOutput(workflow *Workflow, taskId TaskId) error
}

type ChecksInput interface {
	// CheckInput checks the input and if necessary tries to convert into a suitable representation for the executing function
	CheckInput(input map[string]interface{}) error
}

type ReceivesOutput interface {
	// PrepareOutput maps the outputMap of the current node to the inputMap of the next nodes
	PrepareOutput(workflow *Workflow, output map[string]interface{}) error
}

type HasNext interface {
	GetNext() []TaskId
}

type HasNodeType interface {
	GetNodeType() TaskType
}

func Equals[D Task](d1 D, d2 D) bool {
	return d1.Equals(d2)
}
