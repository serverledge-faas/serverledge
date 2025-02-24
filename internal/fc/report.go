package fc

import (
	"fmt"
	"github.com/cornelk/hashmap"
	"github.com/grussorusso/serverledge/internal/function"
	"github.com/grussorusso/serverledge/internal/types"
)

type ExecutionReportId string

func CreateExecutionReportId(dagNode Task) ExecutionReportId {
	return ExecutionReportId(printType(dagNode.GetNodeType()) + "_" + string(dagNode.GetId()))
}

type CompositionExecutionReport struct {
	Result       map[string]interface{}
	Reports      *hashmap.Map[ExecutionReportId, *function.ExecutionReport]
	ResponseTime float64   // time waited by the user to get the output of the entire composition
	Progress     *Progress `json:"-"` // skipped in Json marshaling
}

func (cer *CompositionExecutionReport) GetSingleResult() (string, error) {
	if len(cer.Result) == 1 {
		for _, value := range cer.Result {
			return fmt.Sprintf("%v", value), nil
		}
	}
	return "", fmt.Errorf("there is not exactly one result: there are %d result(s)", len(cer.Result))
}

func (cer *CompositionExecutionReport) GetIntSingleResult() (int, error) {
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

func (cer *CompositionExecutionReport) GetAllResults() string {
	result := "[\n"
	for _, value := range cer.Result {
		result += fmt.Sprintf("\t%v\n", value)
	}
	result += "]\n"
	return result
}

func (cer *CompositionExecutionReport) String() string {
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

func (cer *CompositionExecutionReport) Equals(other types.Comparable) bool {
	cer2, ok := other.(*CompositionExecutionReport)
	if !ok {
		fmt.Printf("other type %T is not CompositionExecutionReport\n", other)
		return false
	}

	allEquals := true
	cer.Reports.Range(func(id ExecutionReportId, report *function.ExecutionReport) bool {
		fmt.Println("Range ")
		report2, isPresent := cer2.Reports.Get(id)
		if !isPresent {
			fmt.Printf("element %s is not present in the other report", id)
			allEquals = false
			return false
		}
		fieldAllEqual := true
		if report.Output != report2.Output {
			fmt.Printf("Output: report1 '%v' is different from report2 '%v'\n", report.Output, report2.Output)
			fieldAllEqual = false
		}

		if report.Duration != report2.Duration {
			fmt.Printf("Duration: report1 '%v' is different from report2 '%v'\n", report.Duration, report2.Duration)
			fieldAllEqual = false
		}

		if report.Result != report2.Result {
			fmt.Printf("Result: report1 '%v' is different from report2 '%v'\n", report.Result, report2.Result)
			fieldAllEqual = false
		}

		if report.OffloadLatency != report2.OffloadLatency {
			fmt.Printf("OffloadLatency: report1 '%v' is different from report2 '%v'\n", report.OffloadLatency, report2.OffloadLatency)
			fieldAllEqual = false
		}

		if report.ResponseTime != report2.ResponseTime {
			fmt.Printf("ResponseTime: report1 '%v' is different from report2 '%v'\n", report.ResponseTime, report2.ResponseTime)
			fieldAllEqual = false
		}

		if report.SchedAction != report2.SchedAction {
			fmt.Printf("SchedAction: report1 '%v' is different from report2 '%v'\n", report.SchedAction, report2.SchedAction)
			fieldAllEqual = false
		}

		if report.InitTime != report2.InitTime {
			fmt.Printf("InitTime: report1 '%v' is different from report2 '%v'\n", report.InitTime, report2.InitTime)
			fieldAllEqual = false
		}

		if report.IsWarmStart != report2.IsWarmStart {
			fmt.Printf("IsWarmStart: report1 '%v' is different from report2 '%v'\n", report.IsWarmStart, report2.IsWarmStart)
			fieldAllEqual = false
		}

		if !fieldAllEqual {
			allEquals = false
			return false
		}
		return true
	})

	if cer.ResponseTime != cer2.ResponseTime {
		fmt.Printf("Composition ResponseTime: %f is different from %f", cer.ResponseTime, cer2.ResponseTime)
		return false
	}

	for key, value := range cer.Result {
		value2, exists := cer2.Result[key]
		if !exists {
			fmt.Printf("Composition Result: key '%s' is not present in the other composition report\n", key)
			return false
		}
		if value != value2 {
			fmt.Printf("Composition Result: value for key '%s' is different from the other composition report. First is %v of type %T, second is %v of type %T\n", key, value, value, value2, value2)
			return false
		}
	}

	return allEquals
}
