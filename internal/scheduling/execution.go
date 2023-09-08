package scheduling

import (
	"fmt"
	"log"
	"time"

	"github.com/serverledge-faas/serverledge/internal/container"
	"github.com/serverledge-faas/serverledge/internal/executor"
	"github.com/serverledge-faas/serverledge/internal/function"
)

const HANDLER_DIR = "/app"

// Execute serves a request on the specified container.
func Execute(contID container.ContainerID, r *scheduledRequest, isWarm bool) (function.ExecutionReport, error) {

	log.Printf("[%s] Executing on container: %v", r.Fun, contID)

	var req executor.InvocationRequest
	if r.Fun.Runtime == container.CUSTOM_RUNTIME {
		req = executor.InvocationRequest{
			Params:       r.Params,
			ReturnOutput: r.ReturnOutput,
		}
	} else {
		cmd := container.RuntimeToInfo[r.Fun.Runtime].InvocationCmd
		req = executor.InvocationRequest{
			Command:      cmd,
			Params:       r.Params,
			Handler:      r.Fun.Handler,
			HandlerDir:   HANDLER_DIR,
			ReturnOutput: r.ReturnOutput,
		}
	}

	t0 := time.Now()
	initTime := t0.Sub(r.Arrival).Seconds()

	response, invocationWait, err := container.Execute(contID, &req)

	if err != nil {
		logs, errLog := container.GetLog(contID)
		if errLog == nil {
			fmt.Println(logs)
		} else {
			fmt.Printf("Failed to get log: %v\n", errLog)
		}

		// notify scheduler
		completions <- &completionNotification{fun: r.Fun, contID: contID, executionReport: nil}
		return function.ExecutionReport{}, fmt.Errorf("[%s] Execution failed on container %v: %v ", r, contID, err)
	}

	if !response.Success {
		// notify scheduler
		completions <- &completionNotification{fun: r.Fun, contID: contID, executionReport: nil}
		return function.ExecutionReport{}, fmt.Errorf("Function execution failed")
	}

	report := function.ExecutionReport{Result: response.Result,
		Output:       response.Output,
		IsWarmStart:  isWarm,
		Duration:     time.Now().Sub(t0).Seconds() - invocationWait.Seconds(),
		ResponseTime: time.Now().Sub(r.Arrival).Seconds()}

	// initializing containers may require invocation retries, adding
	// latency
	report.InitTime = initTime + invocationWait.Seconds()

	// notify scheduler
	completions <- &completionNotification{fun: r.Fun, contID: contID, executionReport: &report}

	return report, nil
}
