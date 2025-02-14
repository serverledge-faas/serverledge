package asl

import (
	"github.com/serverledge-faas/serverledge/internal/types"
	"golang.org/x/exp/slices"
)

// Catch is a field in Task, Parallel and Map states. When a state reports an error and either there is no Retrier, or retries have failed to resolve the error, the interpreter scans through the Catchers in array order, and when the Error Name appears in the value of a Catcher’s "ErrorEquals" field, transitions the machine to the state named in the value of the "Next" field. The reserved name "States.ALL" appearing in a Retrier’s "ErrorEquals" field is a wildcard and matches any Error Name.
type Catch struct {
	ErrorEquals []string
	ResultPath  string
	Next        string
}

func (c *Catch) Equals(cmp types.Comparable) bool {
	c2 := cmp.(*Catch)

	return slices.Equal(c.ErrorEquals, c2.ErrorEquals) &&
		c.ResultPath == c2.ResultPath &&
		c.Next == c2.Next
}

type CanCatch interface {
	GetCatchOpt() Catch
}

func NoCatch() *Catch {
	return &Catch{}
}
