package scheduling

import (
	"fmt"
	"time"

	"github.com/grussorusso/serverledge/internal/container"
	"github.com/grussorusso/serverledge/internal/executor"
)

const HANDLER_DIR = "/app"

// Execute serves a request on the specified container.
func Execute(contID container.ContainerID, r *scheduledRequest) error {
	//log.Printf("[%s] Executing on container: %v", r, contID)

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

	response, invocationWait, err := container.Execute(contID, &req)

	if err != nil {
		// notify scheduler
		completions <- &completion{scheduledRequest: r, contID: contID} // error != nil
		return fmt.Errorf("[%s] Execution failed: %v", r, err)
	}

	if !response.Success {
		// notify scheduler
		completions <- &completion{scheduledRequest: r, contID: contID} // Success == false
		logs, errLogs := container.GetLog(contID)
		if errLogs == nil {
			return fmt.Errorf("\nexecution failed in container - logs of container %s:\n====================\n%s====================", contID, logs) // FIXME: a volte quando ci sono due funzioni diverse, si accede al container con lo stesso runtime, ma non necessariamente con la funzione corretta.
		}
		return fmt.Errorf("execution failed in container - can't read the logs: %v", errLogs)
	}

	r.ExecReport.Result = response.Result
	r.ExecReport.Output = response.Output
	r.ExecReport.Duration = time.Now().Sub(t0).Seconds() - invocationWait.Seconds()
	r.ExecReport.ResponseTime = time.Now().Sub(r.Arrival).Seconds()

	// initializing containers may require invocation retries, adding
	// latency
	r.ExecReport.InitTime += invocationWait.Seconds()

	// notify scheduler
	completions <- &completion{scheduledRequest: r, contID: contID} // Success == true
	return nil
}
