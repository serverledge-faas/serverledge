package workflow

import (
	"time"

	"github.com/grussorusso/serverledge/internal/function"
)

// TODO: offload the entire node when is cloud only
func SubmitCompositionRequest(fcReq *CompositionRequest) error {
	executionReport, err := fcReq.Fc.Invoke(fcReq)
	if err != nil {
		return err
	}
	fcReq.ExecReport = executionReport
	fcReq.ExecReport.ResponseTime = time.Now().Sub(fcReq.Arrival).Seconds()
	return nil
}

// TODO: offload the entire node.
// TODO: make sure the requestId is the one returned from the serverledge node that will execute
func SubmitAsyncCompositionRequest(fcReq *CompositionRequest) {
	executionReport, errInvoke := fcReq.Fc.Invoke(fcReq)
	if errInvoke != nil {
		PublishAsyncCompositionResponse(fcReq.ReqId, CompositionResponse{Success: false})
		return
	}
	reports := make(map[string]*function.ExecutionReport)
	fcReq.ExecReport.Reports.Range(func(id ExecutionReportId, report *function.ExecutionReport) bool {
		reports[string(id)] = report
		return true
	})
	PublishAsyncCompositionResponse(fcReq.ReqId, CompositionResponse{
		Success:      true,
		Result:       fcReq.ExecReport.Result,
		Reports:      reports,
		ResponseTime: fcReq.ExecReport.ResponseTime,
	})
	fcReq.ExecReport = executionReport
	fcReq.ExecReport.ResponseTime = time.Now().Sub(fcReq.Arrival).Seconds()
}
