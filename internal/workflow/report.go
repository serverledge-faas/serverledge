package workflow

import (
	"fmt"
	"github.com/serverledge-faas/serverledge/internal/function"
)

func CreateExecutionReportId(task Task) string {
	return printType(task.GetType()) + "_" + string(task.GetId())
}

type ExecutionReport struct {
	Result       map[string]interface{}
	Reports      map[string]*function.ExecutionReport
	ResponseTime float64 // time waited by the user to get the output of the entire workflow
}

func (cer *ExecutionReport) String() string {
	str := "["
	str += fmt.Sprintf("\n\tResponseTime: %f,", cer.ResponseTime)
	str += "\n\tReports: "

	for id, report := range cer.Reports {
		output := "''"
		if report.Output != "" {
			output = report.Output
		}

		str += fmt.Sprintf("\n\t\t%s: {ResponseTime: %f, IsWarmStart: %v, InitTime: %f, OffloadLatency: %f,"+
			" Duration: %f,  Output: %s, Result: %s}", id, report.ResponseTime, report.IsWarmStart,
			report.InitTime, report.OffloadLatency, report.Duration, output, report.Result)
		str += ",\n"
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
