package scheduling

import (
	"errors"
	"github.com/serverledge-faas/serverledge/internal/container"
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
		p.queue = newFIFOQueue(queueCapacity)
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

	tryDequeueing := !p.queue.isEmpty()

	for tryDequeueing {
		req := p.queue.front()

		containerID, _, err := node.AcquireContainer(req.Fun, true)
		if err == nil {
			p.queue.dequeue()
			log.Printf("[%s] Exec warm from the queue (length=%d)\n", req, p.queue.len())
			execLocally(req, containerID, true)
			continue
		}

		if errors.Is(err, node.NoWarmFoundErr) {
			if node.AcquireResourcesForNewContainer(req.Fun, false) {
				log.Printf("[%s] Cold start from the queue\n", req)
				p.queue.dequeue()

				// This avoids blocking the thread during the cold
				// start, but also allows us to check for resource
				// availability before dequeueing
				node.NewContainerWithAcquiredResourcesAsync(req.Fun, func(c *container.Container) {
					execLocally(req, c, false)
				}, func(e error) {
					dropRequest(req)
				})
			}
		} else if errors.Is(err, node.OutOfResourcesErr) {
			tryDequeueing = false
		} else {
			// other error
			log.Printf("%v", err)
			p.queue.dequeue()
			dropRequest(req)
		}
	}

}

// OnArrival for default policy is executed every time a function is invoked, before invoking the function
func (p *DefaultLocalPolicy) OnArrival(r *scheduledRequest) {
	containerID, warm, err := node.AcquireContainer(r.Fun, false)
	if err == nil {
		execLocally(r, containerID, warm) // decides to execute locally
		return
	}

	if errors.Is(err, node.OutOfResourcesErr) {
		log.Printf("No enough resources to execute %s. Available res: %s\n", r.Fun, &node.LocalResources)
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
		if p.queue.enqueue(r) {
			log.Printf("[%s] Added to queue (length=%d)\n", r, p.queue.len())
			return
		}
	}

	dropRequest(r)
}
