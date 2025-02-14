package asl

import (
	"github.com/serverledge-faas/serverledge/internal/types"
	"golang.org/x/exp/slices"
)

// Retry is a field in Task, Parallel and Map states which retries the state for a period or for a specified number of times
type Retry struct {
	ErrorEquals     []string
	IntervalSeconds int
	BackoffRate     int
	MaxAttempts     int
}

func (r *Retry) Equals(cmp types.Comparable) bool {
	r2 := cmp.(*Retry)
	return slices.Equal(r.ErrorEquals, r2.ErrorEquals) &&
		r.IntervalSeconds == r2.IntervalSeconds &&
		r.BackoffRate == r2.BackoffRate &&
		r.MaxAttempts == r2.MaxAttempts
}

type CanRetry interface {
	GetRetryOpt() Retry
}

func NoRetry() *Retry {
	return &Retry{}
}
