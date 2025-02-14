package workflow

import (
	"log"
	"time"

	"github.com/serverledge-faas/serverledge/internal/function"
)

func SubmitWorkflowInvocationRequest(req *Request) error {
	executionReport, err := req.W.Invoke(req)
	if err != nil {
		return err
	}
	req.ExecReport = executionReport
	req.ExecReport.ResponseTime = time.Now().Sub(req.Arrival).Seconds()
	return nil
}

// TODO: make sure the requestId is the one returned from the serverledge node that will execute
func SubmitAsyncWorkflowInvocationRequest(req *Request) {
	executionReport, errInvoke := req.W.Invoke(req)
	if errInvoke != nil {
		log.Println(errInvoke)
		PublishAsyncInvocationResponse(req.Id, InvocationResponse{Success: false})
		return
	}
	reports := make(map[string]*function.ExecutionReport)
	req.ExecReport.Reports.Range(func(id ExecutionReportId, report *function.ExecutionReport) bool {
		reports[string(id)] = report
		return true
	})
	PublishAsyncInvocationResponse(req.Id, InvocationResponse{
		Success:      true,
		Result:       req.ExecReport.Result,
		Reports:      reports,
		ResponseTime: req.ExecReport.ResponseTime,
	})
	req.ExecReport = executionReport
	req.ExecReport.ResponseTime = time.Now().Sub(req.Arrival).Seconds()
}
