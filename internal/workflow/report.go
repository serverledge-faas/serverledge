package workflow

import (
	"fmt"
	"github.com/cornelk/hashmap"
	"github.com/grussorusso/serverledge/internal/function"
)

type ExecutionReportId string

func CreateExecutionReportId(task Task) ExecutionReportId {
	return ExecutionReportId(printType(task.GetNodeType()) + "_" + string(task.GetId()))
}

type ExecutionReport struct {
	Result       map[string]interface{}
	Reports      *hashmap.Map[ExecutionReportId, *function.ExecutionReport]
	ResponseTime float64   // time waited by the user to get the output of the entire workflow
	Progress     *Progress `json:"-"` // skipped in Json marshaling
}

// TODO: move into test files
func (cer *ExecutionReport) GetSingleResult() (string, error) {
	if len(cer.Result) == 1 {
		for _, value := range cer.Result {
			return fmt.Sprintf("%v", value), nil
		}
	}
	return "", fmt.Errorf("there is not exactly one result: there are %d result(s)", len(cer.Result))
}

// TODO: move into test files
func (cer *ExecutionReport) GetIntSingleResult() (int, error) {
	if len(cer.Result) == 1 {
		for _, value := range cer.Result {
			valueInt, ok := value.(int)
			if !ok {
				return 0, fmt.Errorf("value %v cannot be casted to int", value)
			}
			return valueInt, nil
		}
	}
	return 0, fmt.Errorf("there is not exactly one result: there are %d result(s)", len(cer.Result))
}

func (cer *ExecutionReport) String() string {
	str := "["
	str += fmt.Sprintf("\n\tResponseTime: %f,", cer.ResponseTime)
	str += "\n\tReports: ["
	if cer.Reports.Len() > 0 {
		j := 0
		cer.Reports.Range(func(id ExecutionReportId, report *function.ExecutionReport) bool {
			schedAction := "''"
			if report.SchedAction != "" {
				schedAction = report.SchedAction
			}
			output := "''"
			if report.Output != "" {
				output = report.Output
			}

			str += fmt.Sprintf("\n\t\t%s: {ResponseTime: %f, IsWarmStart: %v, InitTime: %f, OffloadLatency: %f, Duration: %f, SchedAction: %v, Output: %s, Result: %s}", id, report.ResponseTime, report.IsWarmStart, report.InitTime, report.OffloadLatency, report.Duration, schedAction, output, report.Result)
			if j < cer.Reports.Len()-1 {
				str += ","
			}
			if j == cer.Reports.Len()-1 {
				str += "\n\t]"
			}
			j++
			return true
		})
	}

	str += "\n\tResult: {"
	i := 0
	lll := len(cer.Result)
	for s, v := range cer.Result {
		if i == 0 {
			str += "\n"
		}
		str += fmt.Sprintf("\t\t%s: %v,", s, v)
		if i < lll-1 {
			str += ",\n"
		} else if i == lll-1 {
			str += "\n"
		}
		i++
	}
	str += "\t}\n}\n"
	return str
}
