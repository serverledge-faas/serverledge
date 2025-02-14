package asl

import (
	"fmt"

	"github.com/buger/jsonparser"
	"github.com/serverledge-faas/serverledge/internal/types"
)

type StateMachine struct {
	Name    string
	Comment string // Optional
	States  map[string]State
	StartAt string
	Version string // Optional
}

func ParseFrom(name string, aslSrc []byte) (*StateMachine, error) {

	startAt := JsonExtractStringOrDefault(aslSrc, "StartAt", "")

	comment := JsonExtractStringOrDefault(aslSrc, "Comment", "")

	version := JsonExtractStringOrDefault(aslSrc, "Version", "1.0")

	statesData := JsonExtractStringOrDefault(aslSrc, "States", "")

	statesMap, err := parseStates(statesData)
	if err != nil {
		return nil, fmt.Errorf("failed to parse States key: %v", err)
	}

	sm := StateMachine{
		Name:    name,
		StartAt: startAt,
		Comment: comment,
		Version: version,
		States:  statesMap,
	}

	return &sm, nil
}

func (sm *StateMachine) GetAllStateNames() []string {
	// getting all states names
	definedStateNames := make([]string, 0, len(sm.States))
	for stateName := range sm.States {
		definedStateNames = append(definedStateNames, stateName)
	}
	return definedStateNames
}

func (sm *StateMachine) Validate(stateNames []string) error {

	errorStr := ""

	if sm.StartAt == "" {
		errorStr += "invalid ASL: missing StartAt key\n"
	}
	if sm.States == nil {
		errorStr += "invalid ASL: missing States key\n"
	}

	for name, state := range sm.States {
		validatable, ok := state.(Validatable)
		if !ok {
			errorStr = "Validatable should be implemented for all states\n"
		}
		err := validatable.Validate(stateNames)
		if err != nil {
			errorStr = "state " + name + ":" + err.Error() + "\n"
		}
	}
	// return all the errors present in the state machine
	if errorStr != "" {
		return fmt.Errorf("%s", errorStr)
	}

	return nil
}

func emptyParsableFromType(t StateType) Parsable {
	switch t {
	case Task:
		return NewEmptyTask()
	case Choice:
		return NewEmptyChoice()
	case Parallel:
		return NewEmptyParallel()
	case Map:
		return NewEmptyMap()
	case Pass:
		return NewEmptyPass()
	case Wait:
		return NewEmptyWait()
	case Succeed:
		return NewEmptySucceed()
	case Fail:
		return NewEmptyFail()
	default:
		return nil
	}
}

// parseStates is used to create the state machine
func parseStates(statesData string) (map[string]State, error) {
	states := make(map[string]State)
	err := jsonparser.ObjectEach([]byte(statesData), func(key []byte, value []byte, dataType jsonparser.ValueType, offset int) error {
		stateType, err2 := JsonExtractString(value, "Type")
		if err2 != nil {
			return fmt.Errorf("invalid type %s; error: %v", stateType, err2)
		}

		parseable := emptyParsableFromType(StateType(stateType))
		parsedState, err := parseable.ParseFrom(value)
		if err != nil {
			return fmt.Errorf("failed to parse state %s ...\n%v", value[:40], err)
		}
		states[string(key)] = parsedState
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("invalid ASL: %v", err)
	}

	return states, nil
}

func (sm *StateMachine) getStateString() string {
	statesString := "["
	for key, state := range sm.States {
		statesString += "\n\t\t" + key + ": " + state.String()
	}
	if len(sm.States) > 0 {
		statesString += "\n\t]"
	} else {
		statesString += "]" // this will show brackets like this: []
	}
	return statesString
}

func (sm *StateMachine) String() string {

	return fmt.Sprintf("{\n"+
		"\tName: %s\n"+
		"\tComment: %s\n"+
		"\tStartAt: %s\n"+
		"\tVersion: %s\n"+
		"\tStates: "+
		"%s\n"+
		"}",
		sm.Name, sm.Comment, sm.StartAt, sm.Version, sm.getStateString())
}

// GetFunctionNames retrieves all functions defined in the StateMachine, and duplicates are allowed
func (sm *StateMachine) GetFunctionNames() []string {
	funcs := make([]string, 0)
	for _, v := range sm.States {
		res, ok := v.(HasResources)
		if ok {
			funcName := res.GetResources()
			funcs = append(funcs, funcName...)
		}
	}

	return funcs
}

// TODO: delete this functions when you're done with choice
/*func dagBuilding(funcs []*function.Function, currentState State, builder *workflow.DagBuilder, condBuilder *workflow.ChoiceBranchBuilder, stateMachine *StateMachine) (*workflow.Dag, []*function.Function, error) {
	switch currentState.GetType() {
	case "Task":
		return nil, nil, nil
	case "Choice":
		// gets the conditions from matches
		//conds := make([]workflow.Condition, 0)
		//for _, c := range currentState.Choices {
		//	conds = append(conds, c.Operation)
		//}
		//if condBuilder == nil {
		//	condBuilder = builder.AddChoiceNode(conds...)
		//}
		//
		//i := 0
		//for condBuilder.HasNextBranch() {
		//	nextState := stateMachine.States[currentState.Choices[i].Next]
		//	subDag, _, errSub := dagBuilding(funcs, nextState, NewDagBuilder(), condBuilder, stateMachine)
		//	if errSub != nil {
		//		return nil, nil, errSub
		//	}
		//	// assign the function (Task state) to the next branch
		//	condBuilder.NextBranch(subDag, nil)
		//	i++
		//}
		// workflow, err := condBuilder.EndChoiceAndBuild()
		return nil, nil, nil
	case "Fail":
		return nil, nil, nil
	default:
		return nil, nil, fmt.Errorf("unsupported task type: %s", currentState.GetType())
	}
}*/

func (sm *StateMachine) Equals(comparer types.Comparable) bool {
	sm2 := comparer.(*StateMachine)

	if len(sm.States) != len(sm2.States) {
		return false
	}
	// checks if all states are equal
	for k := range sm.States {
		if !sm.States[k].Equals(sm2.States[k]) {
			fmt.Printf("sm.States[k]: %v\n sm2.States[k]: %v\n", sm.States[k], sm2.States[k])
			return false
		}
	}
	return sm.StartAt == sm2.StartAt &&
		sm.Name == sm2.Name &&
		// sm.Comment == sm2.Comment &&
		sm.Version == sm2.Version
}
