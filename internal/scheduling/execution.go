package scheduling

import (
	"fmt"
	"github.com/serverledge-faas/serverledge/internal/node"
	"log"
	"time"

	"github.com/serverledge-faas/serverledge/internal/container"
	"github.com/serverledge-faas/serverledge/internal/executor"
)

const HANDLER_DIR = "/app"

// Execute serves a request on the specified container.
func Execute(cont *container.Container, r *scheduledRequest, isWarm bool) error {

	log.Printf("[%s] Executing on container: %v", r.Fun, cont.ID)

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

	response, invocationWait, err := container.Execute(cont.ID, &req)

	if err != nil {
		logs, errLog := container.GetLog(cont.ID)
		if errLog == nil {
			fmt.Println(logs)
		} else {
			fmt.Printf("Failed to get log: %v\n", errLog)
		}

		// notify scheduler
		completions <- &completionNotification{funcName: r.Fun.Name, offloaded: r.offloaded, cont: cont, failed: true}
		return fmt.Errorf("[%s] Execution failed on container %v: %v ", r, cont.ID, err)
	}

	if !response.Success {
		// notify scheduler
		completions <- &completionNotification{funcName: r.Fun.Name, offloaded: r.offloaded, cont: cont, failed: true}
		return fmt.Errorf("[%s] Function execution failed %v", r, cont.ID)
	}

	r.Result = response.Result
	r.Output = response.Output
	r.IsWarmStart = isWarm
	r.Duration = time.Now().Sub(t0).Seconds() - invocationWait.Seconds()
	r.ResponseTime = time.Now().Sub(r.Arrival).Seconds()
	// initializing containers may require invocation retries, adding // latency
	r.InitTime = initTime + invocationWait.Seconds()
	r.ExecutionArea = node.LocalNode.Area
	r.ExecutionNode = node.LocalNode.Key

	// notify scheduler
	completions <- &completionNotification{funcName: r.Fun.Name, offloaded: r.offloaded, report: *r.ExecutionReport, cont: cont, failed: false}

	return nil
}
