package scheduling

import (
	"fmt"
	"github.com/serverledge-faas/serverledge/internal/registration"
	"log"
	"net/http"
	"runtime"
	"time"

	"github.com/serverledge-faas/serverledge/internal/config"
	"github.com/serverledge-faas/serverledge/internal/container"
	"github.com/serverledge-faas/serverledge/internal/function"
	"github.com/serverledge-faas/serverledge/internal/metrics"
	"github.com/serverledge-faas/serverledge/internal/node"
	"github.com/serverledge-faas/serverledge/internal/telemetry"

	"go.opentelemetry.io/otel/trace"
)

var requests chan *scheduledRequest
var completions chan *completionNotification

var offloadingClient *http.Client

func Run(p Policy) {
	requests = make(chan *scheduledRequest, 500)
	completions = make(chan *completionNotification, 500)

	// initialize resources
	availableCores := runtime.NumCPU()
	node.Resources.AvailableMemMB = int64(config.GetInt(config.POOL_MEMORY_MB, 1024))
	node.Resources.AvailableCPUs = config.GetFloat(config.POOL_CPUS, float64(availableCores))
	node.Resources.ContainerPools = make(map[string]*node.ContainerPool)
	log.Printf("Current resources: %v\n", &node.Resources)

	container.InitDockerContainerFactory()

	//janitor periodically remove expired warm container
	node.GetJanitorInstance()

	tr := &http.Transport{
		MaxIdleConns:        2500,
		MaxIdleConnsPerHost: 2500,
		MaxConnsPerHost:     0,
		IdleConnTimeout:     30 * time.Minute,
	}
	offloadingClient = &http.Client{Transport: tr}

	// initialize scheduling policy
	p.Init()

	log.Println("Scheduler started.")

	var r *scheduledRequest
	var c *completionNotification
	for {
		select {
		case r = <-requests: // receive request
			go p.OnArrival(r)
		case c = <-completions:
			node.HandleCompletion(c.cont, c.fun)
			p.OnCompletion(c.fun, c.executionReport)

			if metrics.Enabled && c.executionReport != nil {
				metrics.AddCompletedInvocation(c.fun.Name)
				if c.executionReport.SchedAction != SCHED_ACTION_OFFLOAD {
					metrics.AddFunctionDurationValue(c.fun.Name, c.executionReport.Duration)
					metrics.AddFunctionInitTimeValue(c.fun.Name, c.executionReport.InitTime)
				}
				outputSize := len(c.executionReport.Result)
				metrics.AddFunctionOutputSizeValue(r.Fun.Name, float64(outputSize))
			}
		}
	}

}

// SubmitRequest submits a newly arrived request for scheduling and execution
func SubmitRequest(r *function.Request) (function.ExecutionReport, error) {
	schedRequest := scheduledRequest{
		Request:         r,
		decisionChannel: make(chan schedDecision, 1)}
	requests <- &schedRequest

	if telemetry.DefaultTracer != nil {
		trace.SpanFromContext(r.Ctx).AddEvent("Scheduling start")
	}

	// wait on channel for scheduling action
	schedDecision, ok := <-schedRequest.decisionChannel
	if !ok {
		return function.ExecutionReport{}, fmt.Errorf("could not schedule the request")
	}
	//log.Printf("[%s] Scheduling decision: %v", r, schedDecision)

	if telemetry.DefaultTracer != nil {
		trace.SpanFromContext(r.Ctx).AddEvent("Scheduling complete")
	}

	if schedDecision.action == DROP {
		//log.Printf("[%s] Dropping request", r)
		return function.ExecutionReport{}, node.OutOfResourcesErr
	} else if schedDecision.action == EXEC_REMOTE {
		//log.Printf("Offloading request")
		return Offload(r, schedDecision.remoteHost)
	} else {
		return Execute(schedDecision.cont, &schedRequest, schedDecision.useWarm)
	}
}

// SubmitAsyncRequest submits a newly arrived async request for scheduling and execution
func SubmitAsyncRequest(r *function.Request) {
	schedRequest := scheduledRequest{
		Request:         r,
		decisionChannel: make(chan schedDecision, 1)}
	requests <- &schedRequest // send async request

	// wait on channel for scheduling action
	schedDecision, ok := <-schedRequest.decisionChannel
	if !ok {
		publishAsyncResponse(r.Id(), function.Response{Success: false})
		return
	}

	var err error
	if schedDecision.action == DROP {
		publishAsyncResponse(r.Id(), function.Response{Success: false})
	} else if schedDecision.action == EXEC_REMOTE {
		//log.Printf("Offloading request\n")
		err = OffloadAsync(r, schedDecision.remoteHost)
		if err != nil {
			publishAsyncResponse(r.Id(), function.Response{Success: false})
		}
	} else {
		report, err := Execute(schedDecision.cont, &schedRequest, schedDecision.useWarm)
		if err != nil {
			publishAsyncResponse(r.Id(), function.Response{Success: false})
			return
		}
		publishAsyncResponse(r.Id(), function.Response{Success: true, ExecutionReport: report})
	}
}

func dropRequest(r *scheduledRequest) {
	r.decisionChannel <- schedDecision{action: DROP}
}

func execLocally(r *scheduledRequest, c *container.Container, warmStart bool) {
	decision := schedDecision{action: EXEC_LOCAL, cont: c, useWarm: warmStart}
	r.decisionChannel <- decision
}

func handleOffload(r *scheduledRequest, serverHost string) {
	r.CanDoOffloading = false // the next server can't offload this request
	r.decisionChannel <- schedDecision{
		action:     EXEC_REMOTE,
		cont:       nil,
		remoteHost: serverHost,
	}
}

func handleCloudOffload(r *scheduledRequest) {
	offloadingTarget := registration.GetRemoteOffloadingTarget()
	if offloadingTarget == nil {
		log.Printf("No remote offloading target available; dropping request")
		r.decisionChannel <- schedDecision{action: DROP}
	} else {
		handleOffload(r, offloadingTarget.APIUrl())
	}
}
