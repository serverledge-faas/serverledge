package lb

import (
	"log"
	"sync"

	"github.com/labstack/echo/v4/middleware"
	"github.com/serverledge-faas/serverledge/internal/container"
	"github.com/serverledge-faas/serverledge/internal/function"
	"github.com/serverledge-faas/serverledge/internal/mab"
)

const MAB = "MAB"
const RR = "RoundRobin"

var AllMemoryAvailable = int64(10_000_000) // A high value to symbolize all memory is free

// MemoryChecker is the function that checks if the node selected has enough memory to execute the function.
// it is an interface, and it's put in HashRing to make unit-tests possible by mocking it
type MemoryChecker interface {
	HasEnoughMemory(target *middleware.ProxyTarget, fun *function.Function) bool
}

type DefaultMemoryChecker struct{}

func (m *DefaultMemoryChecker) HasEnoughMemory(candidate *middleware.ProxyTarget, fun *function.Function) bool {
	freeMemoryMB := NodeMetrics.GetFreeMemory(candidate.Name)
	freeCpu := NodeMetrics.metrics[candidate.Name].FreeCPU
	log.Printf("Candidate has: %d MB free memory. Function needs: %d MB", freeMemoryMB, fun.MemoryMB)
	return freeMemoryMB >= fun.MemoryMB && freeCpu >= fun.CPUDemand

}

var NodeMetrics = &NodeMetricCache{
	metrics: make(map[string]NodeMetric),
}

// ArchitectureCacheLB This map will cache the architecture chosen previously to try and maximize the use of warm containers of targets
var ArchitectureCacheLB = &ArchitectureCache{
	cache: make(map[string]ArchitectureCacheEntry),
}

type NodeMetric struct {
	TotalMemoryMB int64
	FreeMemoryMB  int64
	LastUpdate    int64
	TotalCPU      float64
	FreeCPU       float64
}

type NodeMetricCache struct {
	mu      sync.RWMutex
	metrics map[string]NodeMetric
}

type ArchitectureCacheEntry struct {
	Arch      string
	Timestamp int64
}

type ArchitectureCache struct {
	mu    sync.RWMutex
	cache map[string]ArchitectureCacheEntry
}

// Update info about memory of a specific node. If totalMemMB = 0, then we keep the previous value.
func (c *NodeMetricCache) Update(nodeName string, freeMemMB int64, totalMemMB int64, updateTime int64, freeCpu float64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	curr, ok := c.metrics[nodeName]
	if ok && (updateTime < curr.LastUpdate) {
		return // if this branch is taken, we do not update. The info we already have is "fresher" than the one we received now
	}

	if totalMemMB == 0 && ok {
		totalMemMB = curr.TotalMemoryMB
	}

	c.metrics[nodeName] = NodeMetric{
		TotalMemoryMB: totalMemMB,
		FreeMemoryMB:  freeMemMB,
		LastUpdate:    updateTime,
		FreeCPU:       freeCpu,
	}
}

func (c *NodeMetricCache) GetFreeMemory(nodeName string) int64 {
	c.mu.RLock()
	defer c.mu.RUnlock()

	val, ok := c.metrics[nodeName]
	if !ok {
		// This can probably only happen in the first phases of execution of Serverledge; we have the list of neighbors
		// but we haven't completed yet the first polling round for status information. This means the full system has
		// already started and there should be enough free memory.
		// Plus, these are cloud nodes, so the total memory should be sufficient to execute any function.
		return AllMemoryAvailable
	}

	return val.FreeMemoryMB
}

// Calculate the avg utilization of each architecture
func (b *ArchitectureAwareBalancer) calculateSystemContext() *mab.Context {

	archs := []string{container.ARM, container.X86}
	usageMap := make(map[string]float64)

	for _, arch := range archs {
		var totalFree int64 = 0
		var totalCap int64 = 0

		var nodes []*middleware.ProxyTarget
		if arch == container.ARM {
			nodes = b.armRing.GetAllTargets()
		} else {
			nodes = b.x86Ring.GetAllTargets()
		}

		if len(nodes) == 0 {
			usageMap[arch] = 100.0 // If no node let's assume it's full. This architecture will not be used anyway.
			continue
		}

		for _, node := range nodes {

			NodeMetrics.mu.RLock() // Assumo tu abbia reso pubblico il mutex o aggiunto un metodo Getter completo
			metric, ok := NodeMetrics.metrics[node.Name]
			NodeMetrics.mu.RUnlock()

			if ok && metric.TotalMemoryMB > 0 {
				totalFree += metric.FreeMemoryMB
				totalCap += metric.TotalMemoryMB
			} else {
				log.Printf("[AALB] Node %s has a TotalMemoryMB attribute = 0. It wasn't initialized correctly?\n", node.Name)
				panic(0) // it should never happen
			}
		}

		used := float64(totalCap - totalFree)
		usageMap[arch] = used / float64(totalCap) // % utilization (0.0 - 1.0) fort this specific architecture

	}

	return &mab.Context{ArchMemUsage: usageMap}
}
