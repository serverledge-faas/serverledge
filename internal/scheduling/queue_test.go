package scheduling

import (
	"fmt"
	"testing"

	"github.com/serverledge-faas/serverledge/internal/function"
)

func TestQueuet(t *testing.T) {
	f := function.Function{Name: "Function1"}
	rq := &function.Request{Fun: &f}
	r1 := &scheduledRequest{Request: rq}

	q := NewFIFOQueue(3)
	fmt.Printf("Size = %d\n", q.Len())

	q.Enqueue(r1)
	q.Enqueue(r1)
	fmt.Printf("Size = %d\n", q.Len())
}
