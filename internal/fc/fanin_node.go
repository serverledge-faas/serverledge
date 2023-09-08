package fc

import (
	"fmt"
	"github.com/grussorusso/serverledge/internal/function"
	"github.com/grussorusso/serverledge/internal/types"
	"github.com/lithammer/shortuuid"
	"time"
)

type MergeMode int

const (
	AddNewMapEntry  = iota // The output type will be a map of key-values
	AddToArrayEntry        // The output type will be a map with a single array of values, with repetition
	AddToSetEntry          // The output type will be a map with a single array of unique values
)

// FanInNode receives and merges multiple input and produces a single result
type FanInNode struct {
	Id          DagNodeId
	NodeType    DagNodeType
	BranchId    int
	OutputTo    DagNodeId
	FanInDegree int
	Timeout     time.Duration
	Mode        MergeMode
	// input       []map[string]interface{}
}

var DefaultTimeout = 60 * time.Second

func NewFanInNode(mergeMode MergeMode, fanInDegree int, nillableTimeout *time.Duration) *FanInNode {
	timeout := nillableTimeout
	if timeout == nil {
		timeout = &DefaultTimeout
	}
	fanIn := FanInNode{
		Id:          DagNodeId(shortuuid.New()),
		NodeType:    FanIn,
		OutputTo:    "",
		FanInDegree: fanInDegree,
		Timeout:     *timeout,
		Mode:        mergeMode,
	}

	return &fanIn
}

func (f *FanInNode) Equals(cmp types.Comparable) bool {
	switch f1 := cmp.(type) {
	case *FanInNode:
		return f.Id == f1.Id && f.FanInDegree == f1.FanInDegree && f.OutputTo == f1.OutputTo &&
			f.Timeout == f1.Timeout && f.Mode == f1.Mode
	default:
		return false
	}
}

// Exec already have all inputs when executing, so it simply merges them with the chosen policy
func (f *FanInNode) Exec(compRequest *CompositionRequest, params ...map[string]interface{}) (map[string]interface{}, error) {
	t0 := time.Now()

	fanInOutput := make(map[string]interface{})
	if f.Mode == AddNewMapEntry { // each map entry should have a different name map[i: map[nameI: valueI]]
		duplicates := make(map[string]int)
		for _, inputMap := range params {
			for name, value := range inputMap {
				num, ok := duplicates[name]
				duplicates[name] += 1
				if !ok {
					fanInOutput[name] = value
				} else {
					fanInOutput[fmt.Sprintf("%s_%d", name, num)] = value
				}

			}
		}
	} else if f.Mode == AddToArrayEntry { // all input maps MUST have exactly one entry with the same name
		valid := true
		name := ""
		for _, inputMap := range params {
			if len(inputMap) != 1 {
				return nil, fmt.Errorf("fanIn input map does not have 1 element")
			}
			for k, value := range inputMap {
				if name == "" {
					name = k
					fanInOutput[name] = make([]interface{}, 0)
				} else if name != k {
					valid = false
					break
				}
				fanInOutput[name] = append(fanInOutput[name].([]interface{}), value)
			}
			if valid == false {
				return nil, fmt.Errorf("each fanIn input map must have the same name")
			}
		}
	} else if f.Mode == AddToSetEntry {
		for _, inputMap := range params {
			for name, value := range inputMap {
				_, found := fanInOutput[name]
				if !found {
					fanInOutput[name] = value
				}
			}
		}
	}

	respAndDuration := time.Now().Sub(t0).Seconds()
	execReport := &function.ExecutionReport{
		Result:         fmt.Sprintf("%v", fanInOutput),
		ResponseTime:   respAndDuration,
		IsWarmStart:    true, // not in a container
		InitTime:       0,
		OffloadLatency: 0,
		Duration:       respAndDuration,
		SchedAction:    "",
	}
	//compRequest.ExecReport.Reports[CreateExecutionReportId(f)] = execReport
	compRequest.ExecReport.Reports.Set(CreateExecutionReportId(f), execReport)
	return fanInOutput, nil
}

func (f *FanInNode) AddOutput(dag *Dag, dagNode DagNodeId) error {
	f.OutputTo = dagNode
	return nil
}

// CheckInput doesn't do anything for fan in node
func (f *FanInNode) CheckInput(input map[string]interface{}) error {
	return nil
}

func (f *FanInNode) PrepareOutput(dag *Dag, output map[string]interface{}) error {
	return nil // we should not do nothing, the output should be already ok
}

func (f *FanInNode) GetNext() []DagNodeId {
	// we only have one output
	return []DagNodeId{f.OutputTo}
}

func (f *FanInNode) Width() int {
	return f.FanInDegree
}

func (f *FanInNode) Name() string {
	return "Fan In"
}

func (f *FanInNode) ToString() string {
	return fmt.Sprintf("[FanInNode(%d)]", f.FanInDegree)
}

func (f *FanInNode) setBranchId(number int) {
	f.BranchId = number
}
func (f *FanInNode) GetBranchId() int {
	return f.BranchId
}

func (f *FanInNode) GetId() DagNodeId {
	return f.Id
}

func (f *FanInNode) GetNodeType() DagNodeType {
	return f.NodeType
}
