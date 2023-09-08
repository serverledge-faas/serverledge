package fc

import (
	"fmt"

	"github.com/buger/jsonparser"
	"github.com/grussorusso/serverledge/internal/function"
)

type State struct {
	Name     string
	Type     string
	Next     string
	Resource string
}

type StateMachine struct {
	States       map[string]State
	InitialState string
}

func parseASL(aslSrc []byte) (*StateMachine, error) {
	//stateMachine, err := aslparser.Parse([]byte(aslSrc), false)
	//if err != nil {
	//	return nil, fmt.Errorf("aslparser failed: %v", err)
	//}

	statesData, _, _, err := jsonparser.Get(aslSrc, "States")
	if err != nil {
		return nil, fmt.Errorf("Invalid ASL: %v", err)
	}

	states := make(map[string]State, 0)
	err = jsonparser.ObjectEach(statesData, func(key []byte, value []byte, dataType jsonparser.ValueType, offset int) error {
		stateType, _, _, err2 := jsonparser.Get(value, "Type")
		if err2 == nil {
			s := State{Name: string(key), Type: string(stateType)}
			states[s.Name] = s
			return nil
		} else {
			return err2
		}
	})
	if err != nil {
		return nil, fmt.Errorf("Invalid ASL: %v", err)
	}

	initialState, _, _, err := jsonparser.Get(aslSrc, "StartAt")
	if err != nil {
		return nil, fmt.Errorf("Invalid ASL: %v", err)
	}

	stateMachine := &StateMachine{States: states, InitialState: string(initialState)}
	fmt.Printf("Found state machine:  %v\n", stateMachine)

	return stateMachine, nil
}

func FromASL(name string, aslSrc []byte) (*FunctionComposition, error) {
	stateMachine, err := parseASL(aslSrc)
	if err != nil {
		return nil, fmt.Errorf("could not parse the ASL file: %v", err)
	}

	adj := make(map[string][]string)
	for k, v := range stateMachine.States {
		if v.Type == "Task" {
			adj[k] = make([]string, 1)
			adj[k] = append(adj[k], v.Next)
		} else if v.Type == "Choice" {
			adj[k] = make([]string, 1)
			fmt.Printf("Raw: %v\n", v)
			fmt.Printf("Next: %v\n", v.Next)
			// TODO: adj[k] = append(adj[k], v.Default)
		}
	}
	fmt.Println(adj)

	// TODO: topological sorting
	funcs := make([]*function.Function, 0)
	builder := NewDagBuilder()
	for _, s := range stateMachine.States {
		if s.Type != "Task" {
			return nil, fmt.Errorf("unsupported task type: %s", s.Type) // TODO
		}
		f, found := function.GetFunction(s.Resource)
		if !found {
			return nil, fmt.Errorf("non existing function in composition: %s", s.Resource)
		}
		// get function
		builder = builder.AddSimpleNode(f)
		funcs = append(funcs, f)
		fmt.Printf("Addded simple node with f: %s, funcs=%v", f, funcs)
	}

	dag, err := builder.Build()
	comp := NewFC(name, *dag, funcs, false)
	return &comp, nil
}
