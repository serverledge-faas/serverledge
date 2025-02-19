package scheduling

import "github.com/serverledge-faas/serverledge/internal/function"

type Policy interface {
	Init()
	OnCompletion(fun *function.Function, executionReport *function.ExecutionReport)
	OnArrival(request *scheduledRequest)
}
