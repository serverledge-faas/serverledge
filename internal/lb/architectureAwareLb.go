package lb

import (
	"fmt"
	"log"
	"math/rand"
	"strings"
	"sync"
	"time"

	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	"github.com/lithammer/shortuuid"
	"github.com/serverledge-faas/serverledge/internal/config"
	"github.com/serverledge-faas/serverledge/internal/container"
	"github.com/serverledge-faas/serverledge/internal/function"
	"github.com/serverledge-faas/serverledge/internal/mab"
)

type ArchitectureAwareBalancer struct {
	mu sync.Mutex

	// instead of classic lists we will use hashRings (see hashRing.go) to implement a consistent hashing technique
	armRing *HashRing
	x86Ring *HashRing

	mode      string
	rrIndices map[string]int
}

// NewArchitectureAwareBalancer Constructor
func NewArchitectureAwareBalancer(targets []*middleware.ProxyTarget) *ArchitectureAwareBalancer {

	// REPLICAS is the number of times each physical node will appear in the hash ring. This is done to improve how
	// virtual nodes (i.e.: replicas of each physical node) are distributed over the ring, to reduce variation.
	REPLICAS := config.GetInt(config.REPLICAS, 128)
	log.Printf("Running ArchitectureAwareLB with %d replicas per node in the hash rings\n", REPLICAS)

	b := &ArchitectureAwareBalancer{
		armRing:   NewHashRing(REPLICAS),
		x86Ring:   NewHashRing(REPLICAS),
		rrIndices: make(map[string]int),
	}

	b.mode = config.GetString(config.LB_MODE, RR)
	log.Printf("LB mode set to %s\n", b.mode)

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
func (b *ArchitectureAwareBalancer) Next(c echo.Context) *middleware.ProxyTarget {
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

	targetArch := ""

	var ctx *mab.Context = nil
	if b.mode == MAB {
		ctx = b.calculateSystemContext()           // memory snapshot for the MAB LinUCB
		mab.GlobalContextStorage.Store(reqID, ctx) // Cache it for LinUCB update
	}

	if len(fun.SupportedArchs) == 1 { // If only one architecture is supported skip the MAB and just use that
		targetArch = fun.SupportedArchs[0]
	} else if b.mode == MAB { // if both are supported, then use the MAB to select it
		bandit := mab.GlobalBanditManager.GetBandit(funcName)
		targetArch = bandit.SelectArm(ctx)
	} else if b.mode == RR { // RoundRobin
		targetArch = b.selectArchitectureRR(funcName) // here the load balancer decides what architecture to use for this function
	} else { // Random
		targetArch = b.selectArchitectureRandom() // random load balancer for testing purposes
	}

	// once we selected an architecture, we'll use consistent hashing to select what node to use
	// The Get function will cycle through the hashRing to find a suitable node. If none is find we try to check if in
	// the other ring there is a suitable node for the function, to maximize chances of execution.
	var candidate *middleware.ProxyTarget
	if targetArch == container.ARM { // Prioritize ARM if selected
		candidate = b.armRing.Get(fun)
		if candidate == nil && fun.SupportsArch(container.X86) { // If no ARM node, try x86 if supported
			candidate = b.x86Ring.Get(fun)
		}
	} else { // Prioritize x86 if selected
		candidate = b.x86Ring.Get(fun)
		if candidate == nil && fun.SupportsArch(container.ARM) { // If no x86 node, try ARM if supported
			candidate = b.armRing.Get(fun)
		}
	}
	if candidate != nil {
		freeMemoryMB := NodeMetrics.GetFreeMemory(candidate.Name) - fun.MemoryMB
		// Remove the memory that this function will use (this will then be updated again once the function is executed)
		freeCpu := NodeMetrics.metrics[candidate.Name].FreeCPU - fun.CPUDemand
		NodeMetrics.Update(candidate.Name, freeMemoryMB, 0, time.Now().Unix(), freeCpu)

	}
	return candidate
}

// extractFunctionName retrieves the function's name by parsing the request's URL.
func extractFunctionName(c echo.Context) string {
	path := c.Request().URL.Path

	const prefix = "/invoke/"
	if !strings.HasPrefix(path, prefix) {
		return "" // not an invocation
	}

	return path[len(prefix):]
}

// Deprecated
// This should only be used for tests or as a baseline in experiments.
// selectArchitecture checks the function's runtime to see what architecture it can support. Then it checks if any
// available node of the corresponding architecture is available. If the runtime supports both architecture, then we
// have a tie-break and select a node from the chosen list (arm or x86).
func (b *ArchitectureAwareBalancer) selectArchitecture(fun *function.Function) (string, error) {
	supportsArm := fun.SupportsArch(container.ARM)
	supportsX86 := fun.SupportsArch(container.X86)

	if supportsArm && supportsX86 {
		cacheValidity := 30 * time.Second // may be fine-tuned
		cacheEntry, ok := ArchitectureCacheLB.cache[fun.Name]

		// If we have a valid cache entry, we try to use it
		expiry := time.Unix(cacheEntry.Timestamp, 0).Add(cacheValidity)
		if ok && time.Now().Before(expiry) {
			// Check if the cached architecture has available nodes
			if (cacheEntry.Arch == container.ARM && b.armRing.Size() > 0) ||
				(cacheEntry.Arch == container.X86 && b.x86Ring.Size() > 0) {
				// If the cached architecture is still valid and has available nodes, use it
				cacheEntry.Timestamp = time.Now().Unix() // Update timestamp
				ArchitectureCacheLB.cache[fun.Name] = cacheEntry
				return cacheEntry.Arch, nil
			}

		}

		// Tie-breaking: if both architectures are supported, prefer ARM if available (less energy consumption), otherwise x86.
		// This will also be the fallback if the cached decision is not usable.
		var chosenArch string
		if b.armRing.Size() > 0 {
			chosenArch = container.ARM
		} else if b.x86Ring.Size() > 0 {
			chosenArch = container.X86
		} else {
			return "", fmt.Errorf("no available nodes for either ARM or x86")
		}

		// Update cache
		newCacheEntry := ArchitectureCacheEntry{
			Arch:      chosenArch,
			Timestamp: time.Now().Unix(),
		}
		ArchitectureCacheLB.cache[fun.Name] = newCacheEntry

		return chosenArch, nil
	}

	if supportsArm {
		if b.armRing.Size() > 0 {
			return container.ARM, nil
		}
		return "", fmt.Errorf("no ARM nodes available")
	}

	if supportsX86 {
		if b.x86Ring.Size() > 0 {
			return container.X86, nil
		}
		return "", fmt.Errorf("no x86 nodes available")
	}

	return "", fmt.Errorf("function does not support any available architecture")
}

// selectArchitectureRR selects the architecture using a Round Robin policy.
func (b *ArchitectureAwareBalancer) selectArchitectureRR(funcName string) string {

	// This is just a function to use as a baseline for the LB. It should actually implement checks over the rings dimension.
	// i.e.: it cannot select ARM/X86 "blindly", it should check if we have at least one node for that architecture.
	archs := []string{container.ARM, container.X86}
	index := b.rrIndices[funcName]
	selected := archs[index]
	b.rrIndices[funcName] = (index + 1) % len(archs)
	return selected
}

// AddTarget Echo requires this method for dynamic load-balancing. It simply inserts a new node in the respective ring.
func (b *ArchitectureAwareBalancer) AddTarget(t *middleware.ProxyTarget) bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	nodeInfo := GetSingleTargetInfo(t)
	// Every time we add a node, we set the information about its available memory
	if nodeInfo != nil {
		totalMemoryMb := nodeInfo.TotalMemory
		availMemoryMb := nodeInfo.AvailableMemory
		freeCpu := nodeInfo.TotalCPU - nodeInfo.UsedCPU
		// Update will update the freeMemory only if the information in nodeInfo is fresher than what we
		// already have in the NodeMetrics cache.
		NodeMetrics.Update(t.Name, availMemoryMb, totalMemoryMb, nodeInfo.LastUpdateTime, freeCpu)
	}
	// Decide if target belongs to ARM or x86
	if t.Meta["arch"] == container.ARM {
		b.armRing.Add(t)
	} else {
		b.x86Ring.Add(t)
	}

	return true
}

// RemoveTarget Echo requires this method to remove a target by name
func (b *ArchitectureAwareBalancer) RemoveTarget(name string) bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	delete(NodeMetrics.metrics, name) // this is no longer needed

	if b.armRing.RemoveByName(name) {
		return true
	}
	if b.x86Ring.RemoveByName(name) {
		return true
	}
	return false

}

func (b *ArchitectureAwareBalancer) selectArchitectureRandom() string {
	archs := []string{container.ARM, container.X86}
	// Seed the random number generator if needed, though global rand is usually fine for simple LB
	// rand.Seed(time.Now().UnixNano())
	index := rand.Intn(len(archs))
	return archs[index]
}
