package fc_scheduling

import (
	"github.com/grussorusso/serverledge/internal/fc"
	"time"
)

// TODO: offload the entire node when is cloud only
func SubmitCompositionRequest(fcReq *fc.CompositionRequest) error {
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
func SubmitAsyncCompositionRequest(fcReq *fc.CompositionRequest) {
	executionReport, errInvoke := fcReq.Fc.Invoke(fcReq)
	if errInvoke != nil {
		PublishAsyncCompositionResponse(fcReq.ReqId, fc.CompositionResponse{Success: false})
	}
	PublishAsyncCompositionResponse(fcReq.ReqId, fc.CompositionResponse{Success: true, CompositionExecutionReport: executionReport})
	fcReq.ExecReport = executionReport
	fcReq.ExecReport.ResponseTime = time.Now().Sub(fcReq.Arrival).Seconds()
}
