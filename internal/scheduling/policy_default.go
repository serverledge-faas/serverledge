package scheduling

import (
	"errors"
	"github.com/serverledge-faas/serverledge/internal/function"
	"log"

	"github.com/serverledge-faas/serverledge/internal/config"
	"github.com/serverledge-faas/serverledge/internal/node"
)

// DefaultLocalPolicy can be used on single node deployments. Directly executes the function locally, or drops the request if there aren't enough resources.
type DefaultLocalPolicy struct {
	queue queue
}

func (p *DefaultLocalPolicy) Init() {
	queueCapacity := config.GetInt(config.SCHEDULER_QUEUE_CAPACITY, 0)
	if queueCapacity > 0 {
		log.Printf("Configured queue with capacity %d\n", queueCapacity)
		p.queue = NewFIFOQueue(queueCapacity)
	} else {
		p.queue = nil
	}
}

func (p *DefaultLocalPolicy) OnCompletion(_ *function.Function, _ *function.ExecutionReport) {
	if p.queue == nil {
		return
	}

	p.queue.Lock()
	defer p.queue.Unlock()
	if p.queue.Len() == 0 {
		return
	}

	req := p.queue.Front()

	containerID, err := node.AcquireWarmContainer(req.Fun)
	if err == nil {
		p.queue.Dequeue()
		log.Printf("[%s] Warm start from the queue (length=%d)\n", req, p.queue.Len())
		execLocally(req, containerID, true)
		return
	}

	if errors.Is(err, node.NoWarmFoundErr) {
		if node.AcquireResources(req.Fun.CPUDemand, req.Fun.MemoryMB, true) {
			log.Printf("[%s] Cold start from the queue\n", req)
			p.queue.Dequeue()

			// This avoids blocking the thread during the cold
			// start, but also allows us to check for resource
			// availability before dequeueing
			go func() {
				newContainer, err := node.NewContainerWithAcquiredResources(req.Fun)
				if err != nil {
					dropRequest(req)
				} else {
					execLocally(req, newContainer, false)
				}
			}()
			return
		}
	} else if errors.Is(err, node.OutOfResourcesErr) {
	} else {
		// other error
		log.Printf("%v", err)
		p.queue.Dequeue()
		dropRequest(req)
	}
}

// OnArrival for default policy is executed every time a function is invoked, before invoking the function
func (p *DefaultLocalPolicy) OnArrival(r *scheduledRequest) {
	containerID, err := node.AcquireWarmContainer(r.Fun)
	if err == nil {
		execLocally(r, containerID, true) // decides to execute locally
		return
	}

	if errors.Is(err, node.NoWarmFoundErr) {
		if handleColdStart(r) { // initialize container and executes locally
			return
		}
	} else if errors.Is(err, node.OutOfResourcesErr) {
		// pass
	} else {
		// other error
		dropRequest(r)
		return
	}

	// enqueue if possible
	if p.queue != nil {
		p.queue.Lock()
		defer p.queue.Unlock()
		if p.queue.Enqueue(r) {
			log.Printf("[%s] Added to queue (length=%d)\n", r, p.queue.Len())
			return
		}
	}

	dropRequest(r)
}
