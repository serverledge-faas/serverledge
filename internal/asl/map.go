package asl

import "github.com/serverledge-faas/serverledge/internal/types"

type MapState struct {
	Type      StateType
	InputPath Path
	// ItemsPath is a Reference Path identifying where in the effective input the array field is found.
	ItemsPath Path
	// ItemProcessor is an object that defines a state machine which will process each item or batch of items of the array
	ItemProcessor *StateMachine
	// ItemReader is an object that specifies where to read the items instead of from the effective input
	ItemReader *ItemReaderConf // optional
	// Parameters is like ItemSelector (but is deprecated)
	Parameters string
	// ItemSelector is an object that overrides each single element of the item array
	ItemSelector string // optional
	// ItemBatcher is an object that specifies how to batch the items for the ItemProcessor
	ItemBatcher string // optional
	// ResultWriter is an object that specifies where to write the results instead of to the Map state's result
	ResultWriter string // optional
	// MaxConcurrency is an integer that provides an upper bound on how many invocations of the Iterator may run in parallel
	MaxConcurrency uint32
	// ToleratedFailurePercentage is an integer that provides an upper bound on the percentage of items that may fail
	ToleratedFailurePercentage uint8
	// ToleratedFailureCount is an integer that provides an upper bound on how many items may fail
	ToleratedFailureCount uint8
	// Next is the name of the next state to execute
	Next string
	// End if true, we do not need a Next
	End bool
}

func (m *MapState) Validate(stateNames []string) error {
	//TODO implement me
	panic("implement me")
}

func (m *MapState) IsEndState() bool {
	return m.End
}

func NewEmptyMap() *MapState {
	return &MapState{
		Type:                       Map,
		InputPath:                  "",
		ItemsPath:                  "",
		ItemProcessor:              nil,
		ItemReader:                 nil,
		Parameters:                 "",
		ItemSelector:               "",
		ItemBatcher:                "",
		ResultWriter:               "",
		MaxConcurrency:             0,
		ToleratedFailurePercentage: 0,
		ToleratedFailureCount:      0,
		Next:                       "",
		End:                        false,
	}
}

type ItemReaderConf struct {
	Parameters   PayloadTemplate
	Resource     string
	MaxItems     uint32 // not both
	MaxItemsPath Path   // not both
}

func (m *MapState) GetResources() []string {
	res := make([]string, 0)
	processorResource := func() []string {
		if m.ItemProcessor == nil {
			return []string{}
		}
		return m.ItemProcessor.GetFunctionNames()
	}()
	res = append(res, processorResource...)

	if m.ItemReader != nil && m.ItemReader.Resource != "" {
		res = append(res, m.ItemReader.Resource)
	}

	return res
}

func (m *MapState) Equals(cmp types.Comparable) bool {
	m2 := cmp.(*MapState)
	return m.Type == m2.Type
}

func (m *MapState) ParseFrom(jsonData []byte) (State, error) {
	//TODO implement me
	panic("implement me")
}

func (m *MapState) GetNext() (string, bool) {
	if m.End == false {
		return m.Next, true
	}
	return "", false
}

func (m *MapState) GetType() StateType {
	return Map
}

func (m *MapState) String() string {
	return "Map"
}
