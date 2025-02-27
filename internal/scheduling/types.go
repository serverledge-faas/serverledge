package scheduling

import (
	"github.com/grussorusso/serverledge/internal/container"
	"github.com/grussorusso/serverledge/internal/function"
)

// scheduledRequest represents a Request within the scheduling subsystem
type scheduledRequest struct {
	*function.Request
	decisionChannel chan schedDecision
	priority        float64
}

type completion struct {
	*scheduledRequest
	contID container.ContainerID
}

// schedDecision wraps a action made by the scheduler.
// Possible decisions are 1) drop, 2) execute locally or 3) execute on a remote
// Node (offloading).
type schedDecision struct {
	action     action
	contID     container.ContainerID
	remoteHost string
}

type action int64

const (
	DROP                  action = 0
	EXEC_LOCAL                   = 1
	EXEC_REMOTE                  = 2
	BEST_EFFORT_EXECUTION        = 3
)

type schedulingDecision int64

const (
	SCHED_DROP   schedulingDecision = 0
	SCHED_REMOTE                    = 1
	SCHED_LOCAL                     = 2
	SCHED_BASIC                     = 3
)
