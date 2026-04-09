package lb

import (
	"log"
	"sync"
	"time"

	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	"github.com/lithammer/shortuuid"
	"github.com/serverledge-faas/serverledge/internal/config"
	"github.com/serverledge-faas/serverledge/internal/container"
	"github.com/serverledge-faas/serverledge/internal/function"
)

type ArchitectureUNawareBalancer struct {
	mu sync.Mutex

	// instead of classic lists we will use hashRings (see hashRing.go) to implement a consistent hashing technique
	hashRing *HashRing
}

// NewArchitectureUNawareBalancer Constructor
func NewArchitectureUNawareBalancer(targets []*middleware.ProxyTarget) *ArchitectureUNawareBalancer {

	// REPLICAS is the number of times each physical node will appear in the hash ring. This is done to improve how
	// virtual nodes (i.e.: replicas of each physical node) are distributed over the ring, to reduce variation.
	REPLICAS := config.GetInt(config.REPLICAS, 128)
	log.Printf("Running ArchitectureUNawareLB with %d replicas per node in the hash ring\n", REPLICAS)

	b := &ArchitectureUNawareBalancer{
		hashRing: NewHashRing(REPLICAS),
	}

	log.Printf("Starting Architecture UNAWARE Lb\n")

	// to stay consistent with the old RoundRobinLoadBalancer, we'll still a single target list, that will contain all nodes,
	// both ARM and x86. We will now sort them into the respective hashRings.
	for _, t := range targets {
		arch := t.Meta["arch"]
		if arch == container.ARM || arch == container.X86 {
			b.AddTarget(t)
		} else {
			log.Printf("Unknown architecture for node %s\n", t.Name)
		}
	}

	return b
}

// Next Used by Echo Proxy middleware to select the next target dynamically
func (b *ArchitectureUNawareBalancer) Next(c echo.Context) *middleware.ProxyTarget {
	b.mu.Lock()
	defer b.mu.Unlock()

	funcName := extractFunctionName(c)        // get function's name from request's URL
	fun, ok := function.GetFunction(funcName) // we use this to leverage cache before asking etcd
	if !ok {
		log.Printf("Dropping request for unknown fun '%s'\n", funcName)
		return nil
	}

	reqID := shortuuid.New()
	c.Request().Header.Set("Serverledge-MAB-Request-ID", reqID)

	candidate := b.hashRing.Get(fun)

	if candidate != nil {
		freeMemoryMB := NodeMetrics.GetFreeMemory(candidate.Name) - fun.MemoryMB
		// Remove the memory that this function will use (this will then be updated again once the function is executed)
		freeCpu := NodeMetrics.metrics[candidate.Name].FreeCPU - fun.CPUDemand
		NodeMetrics.Update(candidate.Name, freeMemoryMB, 0, time.Now().Unix(), freeCpu)

	}
	return candidate
}

// AddTarget Echo requires this method for dynamic load-balancing. It simply inserts a new node in the respective ring.
func (b *ArchitectureUNawareBalancer) AddTarget(t *middleware.ProxyTarget) bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	nodeInfo := GetSingleTargetInfo(t)
	// Every time we add a node, we set the information about its available memory
	if nodeInfo != nil {
		totalMemoryMb := nodeInfo.TotalMemory
		freeMemoryMB := totalMemoryMb - nodeInfo.UsedMemory
		freeCpu := nodeInfo.TotalCPU - nodeInfo.UsedCPU
		// Update will update the freeMemory only if the information in nodeInfo is fresher than what we
		// already have in the NodeMetrics cache.
		NodeMetrics.Update(t.Name, freeMemoryMB, totalMemoryMb, nodeInfo.LastUpdateTime, freeCpu)
	}

	b.hashRing.Add(t)

	return true
}

// RemoveTarget Echo requires this method to remove a target by name
func (b *ArchitectureUNawareBalancer) RemoveTarget(name string) bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	delete(NodeMetrics.metrics, name) // this is no longer needed

	if b.hashRing.RemoveByName(name) {
		return true
	}
	return false

}
