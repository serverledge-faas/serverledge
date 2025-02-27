package workflow

import (
	"time"

	"github.com/grussorusso/serverledge/internal/function"
)

// TODO: offload the entire node when is cloud only
func SubmitWorkflowInvocationRequest(req *Request) error {
	executionReport, err := req.W.Invoke(req)
	if err != nil {
		return err
	}
	req.ExecReport = executionReport
	req.ExecReport.ResponseTime = time.Now().Sub(req.Arrival).Seconds()
	return nil
}

// TODO: offload the entire node.
// TODO: make sure the requestId is the one returned from the serverledge node that will execute
func SubmitAsyncWorkflowInvocationRequest(req *Request) {
	executionReport, errInvoke := req.W.Invoke(req)
	if errInvoke != nil {
		PublishAsyncInvocationResponse(req.ReqId, InvocationResponse{Success: false})
		return
	}
	reports := make(map[string]*function.ExecutionReport)
	req.ExecReport.Reports.Range(func(id ExecutionReportId, report *function.ExecutionReport) bool {
		reports[string(id)] = report
		return true
	})
	PublishAsyncInvocationResponse(req.ReqId, InvocationResponse{
		Success:      true,
		Result:       req.ExecReport.Result,
		Reports:      reports,
		ResponseTime: req.ExecReport.ResponseTime,
	})
	req.ExecReport = executionReport
	req.ExecReport.ResponseTime = time.Now().Sub(req.Arrival).Seconds()
}
