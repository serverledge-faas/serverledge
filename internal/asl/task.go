package asl

import (
	"fmt"

	"github.com/serverledge-faas/serverledge/internal/types"
)

type TaskState struct {
	Type                 StateType       // Necessary
	Resource             string          // Necessary
	Next                 string          // Necessary when End = false
	Parameters           PayloadTemplate // Optional
	InputPath            Path            // Optional
	OutputPath           Path            // Optional
	ResultPath           Path            // Optional
	ResultSelector       PayloadTemplate // Optional
	Retry                *Retry          // Optional
	Catch                *Catch          // Optional
	TimeoutSeconds       uint32          // Optional, in seconds
	TimeoutSecondsPath   Path            // Optional, in seconds
	HeartbeatSeconds     uint32          // Optional, in seconds. Smaller than timeoutSeconds
	HeartbeatSecondsPath Path            // Optional, in seconds. Smaller than timeoutSeconds
	End                  bool            // default false
}

func (t *TaskState) ParseFrom(jsonData []byte) (State, error) {
	var err error
	t.Resource = JsonExtractStringOrDefault(jsonData, "Resource", "")

	t.End = JsonExtractBool(jsonData, "End")

	t.Next = JsonExtractStringOrDefault(jsonData, "Next", "")

	t.Parameters.json = JsonExtractStringOrDefault(jsonData, "Parameters", "")

	t.InputPath = JsonExtractRefPathOrDefault(jsonData, "InputPath", "")
	t.OutputPath = JsonExtractRefPathOrDefault(jsonData, "OutputPath", "")
	t.ResultPath = JsonExtractRefPathOrDefault(jsonData, "ResultPath", "")

	t.ResultSelector.json = JsonExtractStringOrDefault(jsonData, "ResultSelector", "")
	t.Retry = JsonExtractObjectOrDefault(jsonData, "Retry", &Retry{}).(*Retry)
	t.Catch = JsonExtractObjectOrDefault(jsonData, "Catch", &Catch{}).(*Catch)

	t.TimeoutSeconds = uint32(JsonExtractIntOrDefault(jsonData, "TimeoutSeconds", 0))
	t.TimeoutSecondsPath, err = JsonTryExtractRefPath(jsonData, "TimeoutSecondsPath")
	if err != nil {
		return nil, err
	}
	t.HeartbeatSeconds = uint32(JsonExtractIntOrDefault(jsonData, "HeartbeatSeconds", 0))
	t.HeartbeatSecondsPath, err = JsonTryExtractRefPath(jsonData, "HeartbeatSecondsPath")
	if err != nil {
		return nil, err
	}
	return t, nil
}

func (t *TaskState) Validate(stateNames []string) error {
	if t.Resource == "" {
		return fmt.Errorf("resource field is mandatory for a task state, but it is not defined")
	}
	if t.End == false && t.Next == "" {
		return fmt.Errorf("next field is mandatory for a non-terminal task state, but it is not defined")
	}
	if t.End == true && t.Next != "" {
		return fmt.Errorf("next field should not be defined for a terminal task state, but it is")
	}
	if t.HeartbeatSeconds > t.TimeoutSeconds {
		return fmt.Errorf("HeartbeatSeconds %d exceeds timeout %d", t.HeartbeatSeconds, t.TimeoutSeconds)
	}

	if t.TimeoutSecondsPath != "" && t.TimeoutSeconds != 0 {
		return fmt.Errorf("TimeoutSecondsPath and TimeoutSeconds cannot be set at the same time")
	}

	if t.HeartbeatSecondsPath != "" && t.HeartbeatSeconds != 0 {
		return fmt.Errorf("HeartbeatSecondsPath and HeartbeatSeconds cannot be set at the same time")
	}

	return nil
}

func NewTerminalTask(resource string) *TaskState {
	return &TaskState{
		Type:                 Task,
		Resource:             resource,
		Next:                 "",
		Parameters:           PayloadTemplate{""},
		InputPath:            "",
		OutputPath:           "",
		ResultPath:           "",
		ResultSelector:       PayloadTemplate{""},
		Retry:                NoRetry(),
		Catch:                NoCatch(),
		TimeoutSeconds:       0,
		TimeoutSecondsPath:   "",
		HeartbeatSeconds:     0,
		HeartbeatSecondsPath: "",
		End:                  true,
	}
}

func NewNonTerminalTask(resource string, next string) *TaskState {
	return &TaskState{
		Type:                 Task,
		Resource:             resource,
		Next:                 next,
		Parameters:           PayloadTemplate{""},
		InputPath:            "",
		OutputPath:           "",
		ResultPath:           "",
		ResultSelector:       PayloadTemplate{""},
		Retry:                NoRetry(),
		Catch:                NoCatch(),
		TimeoutSeconds:       0,
		TimeoutSecondsPath:   "",
		HeartbeatSeconds:     0,
		HeartbeatSecondsPath: "",
		End:                  false,
	}
}

func NewEmptyTask() *TaskState {
	return &TaskState{
		Type:                 Task,
		Resource:             "",
		Next:                 "",
		Parameters:           PayloadTemplate{""},
		InputPath:            "",
		OutputPath:           "",
		ResultPath:           "",
		ResultSelector:       PayloadTemplate{""},
		Retry:                nil,
		Catch:                nil,
		TimeoutSeconds:       0,
		TimeoutSecondsPath:   "",
		HeartbeatSeconds:     0,
		HeartbeatSecondsPath: "",
		End:                  false,
	}
}

func (t *TaskState) GetType() StateType {
	return Task
}

func (t *TaskState) GetNext() (string, bool) {
	if t.End == false {
		return t.Next, true
	}
	return "", false
}

func (t *TaskState) GetResources() []string {
	return []string{t.Resource}
}

func (t *TaskState) IsEndState() bool {
	return t.End
}

func (t *TaskState) Equals(cmp types.Comparable) bool {
	t2, ok := cmp.(*TaskState)
	if !ok {
		fmt.Printf("t1: %v\nt2: %v\n", t, cmp)
		return false
	}
	return t.Type == t2.Type &&
		t.Resource == t2.Resource &&
		t.Next == t2.Next &&
		t.Parameters.json == t2.Parameters.json &&
		t.InputPath == t2.InputPath &&
		t.OutputPath == t2.OutputPath &&
		t.ResultPath == t2.ResultPath &&
		t.ResultSelector.json == t2.ResultSelector.json &&
		t.Retry.Equals(t2.Retry) &&
		t.Catch.Equals(t2.Catch) &&
		t.TimeoutSeconds == t2.TimeoutSeconds &&
		t.TimeoutSecondsPath == t2.TimeoutSecondsPath &&
		t.HeartbeatSeconds == t2.HeartbeatSeconds &&
		t.HeartbeatSecondsPath == t2.HeartbeatSecondsPath &&
		t.End == t2.End
}

func (t *TaskState) String() string {
	str := fmt.Sprint("{",
		"\n\t\t\tType: ", t.Type,
		"\n\t\t\tResource: ", t.Resource,
		"\n")

	if t.Next != "" {
		str += fmt.Sprintf("\t\t\tNext: %s\n", t.Next)
	}

	if t.Parameters.json != "" {
		str += fmt.Sprintf("\t\t\tParameters: %s\n", t.Parameters.json)
	}

	if t.InputPath != "" {
		str += fmt.Sprintf("\t\t\tInputPath: %s\n", t.InputPath)
	}
	if t.OutputPath != "" {
		str += fmt.Sprintf("\t\t\tOutputPath: %s\n", t.OutputPath)
	}
	if t.ResultPath != "" {
		str += fmt.Sprintf("\t\t\tResultPath: %s\n", t.ResultPath)
	}

	if t.ResultSelector.json != "" {
		str += fmt.Sprint("\t\t\tResultSelector: ", t.ResultSelector)
	}
	if t.Retry != nil {
		str += fmt.Sprintf("\t\t\tRetry: %v\n", t.Retry)
	}
	if t.Catch != nil {
		str += fmt.Sprintf("\t\t\tCatch: %v\n", t.Catch)
	}
	if t.TimeoutSeconds != 0 {
		str += fmt.Sprintf("\t\t\tTimeoutSeconds: %d\n", t.TimeoutSeconds)
	}
	if t.TimeoutSecondsPath != "" {
		str += fmt.Sprintf("\t\t\tTimeoutSecondsPath: %s\n", t.TimeoutSecondsPath)
	}
	if t.HeartbeatSeconds != 0 {
		str += fmt.Sprintf("\t\t\tHeartbeatSeconds: %d\n", t.HeartbeatSeconds)
	}
	if t.HeartbeatSecondsPath != "" {
		str += fmt.Sprintf("\t\t\tHeartbeatSecondsPath: %s\n", t.HeartbeatSecondsPath)
	}
	if t.End != false {
		str += fmt.Sprintf("\t\t\tEnd: %v\n", t.End)
	}
	str += "\t\t}"
	return str
}
