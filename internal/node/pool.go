package node

import (
	"errors"
	"log"
	"time"

	"github.com/serverledge-faas/serverledge/internal/config"
	"github.com/serverledge-faas/serverledge/internal/container"
	"github.com/serverledge-faas/serverledge/internal/function"
)

type ContainerPool struct {
	// for better efficiently we now use slices here instead of linked lists
	busy []*container.Container
	idle []*container.Container
}

var NoWarmFoundErr = errors.New("no warm container is available")

// GetContainerPool retrieves (or creates) the container pool for a function.
func GetContainerPool(f *function.Function) *ContainerPool {
	if fp, ok := LocalResources.containerPools[f.Name]; ok {
		return fp
	}

	fp := newContainerPool()
	LocalResources.containerPools[f.Name] = fp
	return fp
}

func (fp *ContainerPool) popIdleContainer() (*container.Container, bool) {
	n := len(fp.idle)
	if n == 0 {
		return nil, false
	}
	// LIFO (maybe better for cache locality)
	c := fp.idle[n-1]

	fp.idle[n-1] = nil      // to favor garbage collection
	fp.idle = fp.idle[:n-1] // pop the slice

	return c, true
}

func (fp *ContainerPool) getReusableContainer(maxConcurrency int16) (*container.Container, bool) {
	for _, elem := range fp.busy {
		c := elem
		if c.RequestsCount < maxConcurrency {
			return c, true
		}
	}

	return nil, false
}

func newContainerPool() *ContainerPool {
	return &ContainerPool{
		busy: make([]*container.Container, 0, 10),
		idle: make([]*container.Container, 0, 10),
	}
}

func acquireNewMemory(mem int64, forWarmPool bool) bool {
	if LocalResources.AvailableMemory() < mem {
		return false
	}

	if LocalResources.FreeMemory() < mem {
		ok, err := dismissContainer(mem - LocalResources.FreeMemory())
		if err != nil || !ok {
			return false
		}
	}

	if forWarmPool {
		LocalResources.warmPoolUsedMem += mem
	} else {
		LocalResources.busyPoolUsedMem += mem
	}

	return true
}

// acquireWarmContainer acquires a warm container for a given function (if any).
// A warm container is in running/paused state and has already been initialized
// with the function code.
// The function returns an error if either:
// (i) the warm container does not exist
// (ii) there are not enough resources to start the container
func acquireWarmContainer(f *function.Function) (*container.Container, error) {
	LocalResources.Lock()
	defer LocalResources.Unlock()

	fp := GetContainerPool(f)

	if f.MaxConcurrency > 1 {
		// 1. try to reuse a running container
		c, found := fp.getReusableContainer(f.MaxConcurrency)
		if found {
			c.RequestsCount += 1
			log.Printf("Re-Using busy %s for %s. ", c.ID, f)
			return c, nil
		}
	}

	// 2. try to pick a idle container
	c, found := fp.popIdleContainer()
	if !found {
		return nil, NoWarmFoundErr
	}

	if LocalResources.AvailableCPUs() < f.CPUDemand {
		//log.Printf("Not enough CPU to start a warm container for %s", f)
		return nil, OutOfResourcesErr
	}
	LocalResources.busyPoolUsedMem += f.MemoryMB
	LocalResources.warmPoolUsedMem -= f.MemoryMB
	LocalResources.usedCPUs += f.CPUDemand

	// add container to the busy pool
	c.RequestsCount = 1
	fp.busy = append(fp.busy, c)

	log.Printf("Using warm %s for %s. Now: %v", c.ID, f, &LocalResources)
	return c, nil
}

func AcquireContainer(f *function.Function, onlyIfWarm bool) (*container.Container, bool, error) {
	c, err := acquireWarmContainer(f)
	if err == nil {
		return c, true, nil
	} else if !errors.Is(err, NoWarmFoundErr) {
		return nil, true, err
	}

	if onlyIfWarm {
		return nil, false, NoWarmFoundErr
	}

	// Cold start required
	if !AcquireResourcesForNewContainer(f, false) {
		return nil, false, OutOfResourcesErr
	}
	c, err = NewContainerWithAcquiredResources(f, false, false)
	return c, false, err
}

func HandleCompletion(cont *container.Container, f *function.Function) {
	LocalResources.Lock()
	defer LocalResources.Unlock()

	cont.RequestsCount--
	if cont.RequestsCount == 0 {
		// the container is now idle and must be moved to the warm pool
		fp := GetContainerPool(f)
		// Search the container index in the slice
		idx := -1
		for i, c := range fp.busy {
			if c == cont { // with slices, we can compare pointers
				idx = i
				break
			}
		}

		if idx == -1 {
			log.Println("Failed to release a container! Not found in busy pool.")
			return
		}

		// swap then pop from the slice. This way we don't have to
		lastIdx := len(fp.busy) - 1
		fp.busy[idx] = fp.busy[lastIdx] // swap between last element and the one we want to delete
		fp.busy[lastIdx] = nil          // nil to favor garbage collection
		fp.busy = fp.busy[:lastIdx]     // pop the slice

		// finally, we add the container to the idle pool
		d := time.Duration(config.GetInt(config.CONTAINER_EXPIRATION_TIME, 600)) * time.Second
		cont.ExpirationTime = time.Now().Add(d).UnixNano()
		fp.idle = append(fp.idle, cont)

		LocalResources.usedCPUs -= f.CPUDemand
		LocalResources.busyPoolUsedMem -= f.MemoryMB
		LocalResources.warmPoolUsedMem += f.MemoryMB
	}
}

func AcquireResourcesForNewContainer(fun *function.Function, forWarmPool bool) bool {
	LocalResources.Lock()
	defer LocalResources.Unlock()

	if LocalResources.AvailableCPUs() < fun.CPUDemand {
		return false
	}
	ok := acquireNewMemory(fun.MemoryMB, forWarmPool)
	if !ok {
		return false
	}

	if !forWarmPool {
		LocalResources.usedCPUs += fun.CPUDemand
	}
	return true
}

// NewContainerWithAcquiredResources spawns a new container for the given
// function, assuming that the required CPU and memory resources have been
// already been acquired.
func NewContainerWithAcquiredResources(fun *function.Function, startAsIdle bool, forceImagePull bool) (*container.Container, error) {
	cont, err := container.CreateContainer(fun, forceImagePull)

	if err != nil {
		log.Printf("Failed container creation: %v\n", err)
	}

	LocalResources.Lock()
	defer LocalResources.Unlock()
	if err != nil {
		LocalResources.busyPoolUsedMem -= fun.MemoryMB
		LocalResources.usedCPUs -= fun.CPUDemand
		return nil, err
	}

	fp := GetContainerPool(fun)
	if startAsIdle {
		fp.idle = append(fp.idle, cont)
	} else {
		cont.RequestsCount = 1
		fp.busy = append(fp.busy, cont) // We immediately mark it as busy
	}

	return cont, nil
}

func NewContainerWithAcquiredResourcesAsync(fun *function.Function, okCallback func(cont *container.Container), errCallback func(e error)) {
	go func() {
		cont, err := container.CreateContainer(fun, false)
		if err != nil {
			log.Printf("Failed container creation: %v\n", err)
			errCallback(err)
			return
		}

		LocalResources.Lock()
		defer LocalResources.Unlock()
		if err != nil {
			LocalResources.busyPoolUsedMem -= fun.MemoryMB
			LocalResources.usedCPUs -= fun.CPUDemand
			return
		}

		fp := GetContainerPool(fun)
		cont.RequestsCount = 1
		fp.busy = append(fp.busy, cont) // We immediately mark it as busy
		okCallback(cont)
	}()
}

type itemToDismiss struct {
	cont   *container.Container
	pool   *ContainerPool
	memory int64
}

// removeContainerFromIdle removes a specific container from the idle pool.
func (fp *ContainerPool) removeContainerFromIdle(target *container.Container) bool {
	for i, c := range fp.idle {
		if c == target { // pointer comparison
			// swap with the last element
			lastIdx := len(fp.idle) - 1
			fp.idle[i] = fp.idle[lastIdx]

			fp.idle[lastIdx] = nil // for better garbage collection we delete the last element content before slicing

			fp.idle = fp.idle[:lastIdx]
			return true
		}
	}
	return false
}

// dismissContainer attempts to free memory by dismissing idle containers.
// It works in 2 phases: research (collect candidates) and cleanup (destroy them).
// Containers are actually cleaned only if they free enough memory for the new function/container
func dismissContainer(requiredMemoryMB int64) (bool, error) {
	log.Printf("Trying to dismiss containers to free up at least %d MB", requiredMemoryMB)
	var cleanedMB int64 = 0
	var containerToDismiss []itemToDismiss

	// Phase 1: Research
	// We iterate through all pools to find idle containers to (potentially) remove.
	for _, funPool := range LocalResources.containerPools {

		if len(funPool.idle) > 0 {
			for _, cont := range funPool.idle {
				memory, _ := container.GetMemoryMB(cont.ID)

				// We collect the pointer to the container and the pool reference
				containerToDismiss = append(containerToDismiss,
					itemToDismiss{cont: cont, pool: funPool, memory: memory})

				cleanedMB += memory
				if cleanedMB >= requiredMemoryMB {
					goto cleanup
				}
			}
		}
	}

cleanup: // Phase 2: Cleanup
	if cleanedMB >= requiredMemoryMB { // if we'd actually free enough memory we do it, otherwise there's no point
		for _, item := range containerToDismiss {
			if item.pool.removeContainerFromIdle(item.cont) {
				// Destroy the actual container resources (Docker/Containerd)
				err := container.Destroy(item.cont.ID)
				if err != nil {
					return false, err
				}
				LocalResources.warmPoolUsedMem -= item.memory
			}
		}
	} else {
		log.Printf("Not enough containers to free up at least %d MB (avail to dismiss: %d)", requiredMemoryMB, cleanedMB)
		return false, errors.New("not enough containers to free up memory")

	}
	return true, nil
}

// DeleteExpiredContainer is called by the container cleaner
// Deletes expired warm container
func DeleteExpiredContainer() {
	now := time.Now().UnixNano()

	LocalResources.Lock()
	defer LocalResources.Unlock()

	for _, pool := range LocalResources.containerPools {
		// Index to track the position of kept elements
		// Basically, since we now have slices, we want to avoid modifying the slice length while we're iterating
		// over it. So, we keep track of the element (containers) we want to keep, then we move them to the front of
		// the slice, and finally we cut the slice.
		// E.g.: if we have 8 containers, but 3 of them are expired, we cycle over the 8 containers, we put the 5 we need
		// to keep from the index 0 to 4, and then we cut the slice after the fifth element.
		// Finally, we "nil-out" the last 3 elements (in this example), to favor garbage collection.
		kept := 0

		for _, warm := range pool.idle {
			if now > warm.ExpirationTime {
				// remove the expired container

				// Update resources
				memory, _ := container.GetMemoryMB(warm.ID)
				LocalResources.warmPoolUsedMem -= memory

				// Destroy the actual container
				err := container.Destroy(warm.ID)
				if err != nil {
					log.Printf("Error while destroying container %s: %s\n", warm.ID, err)
				}

			} else {
				// container is still valid: Keep it
				// Rewrite the element at the 'kept' position
				pool.idle[kept] = warm
				kept++ // position to write the (eventual) next container we need to keep
			}
		}

		// finally, we set as nil the references to the containers we deleted
		for i := kept; i < len(pool.idle); i++ {
			pool.idle[i] = nil
		}

		// Reslice to the new length
		pool.idle = pool.idle[:kept]
	}
}

// ShutdownWarmContainersFor destroys warm containers of a given function
// Actual termination happens asynchronously.
func ShutdownWarmContainersFor(f *function.Function) {
	LocalResources.Lock()
	defer LocalResources.Unlock()

	fp, ok := LocalResources.containerPools[f.Name]
	if !ok {
		return
	}

	containersToDelete := make([]container.ContainerID, 0, len(fp.idle)) // we already know how long it'll need to be, so no need for reallocation

	// Iterate over the idle slice directly
	for i, warmed := range fp.idle {
		log.Printf("Removing container with ID %s\n", warmed.ID)

		memory, _ := container.GetMemoryMB(warmed.ID)
		LocalResources.warmPoolUsedMem -= memory
		containersToDelete = append(containersToDelete, warmed.ID)
		fp.idle[i] = nil
	}

	// clear the slice
	fp.idle = fp.idle[:0]

	go func(contIDs []container.ContainerID) {
		for _, contID := range contIDs {
			// No need to update available resources here
			if err := container.Destroy(contID); err != nil {
				log.Printf("An error occurred while deleting %s: %v\n", contID, err)
			} else {
				log.Printf("Deleted %s\n", contID)
			}
		}
	}(containersToDelete)
}

// ShutdownAllContainers destroys all container (usually on termination)
func ShutdownAllContainers() {
	LocalResources.Lock()
	defer LocalResources.Unlock()

	for fun, pool := range LocalResources.containerPools {
		functionDescriptor, _ := function.GetFunction(fun)
		if functionDescriptor == nil {
			log.Printf("Could not find function, cannot shutdown containers: %s\n", fun)
			continue // should not happen
		}

		for i, warmed := range pool.idle {
			log.Printf("Removing container with ID %s\n", warmed.ID)

			err := container.Destroy(warmed.ID)
			if err != nil {
				log.Printf("Error while destroying container %s: %s", warmed.ID, err)
			}
			LocalResources.warmPoolUsedMem -= functionDescriptor.MemoryMB

			// nil to help garbage collection
			pool.idle[i] = nil
		}
		// Reset the idle slice
		pool.idle = pool.idle[:0]

		// now we do the same but for busy containers
		for i, busyCont := range pool.busy {
			log.Printf("Removing container with ID %s\n", busyCont.ID)

			err := container.Destroy(busyCont.ID)
			if err != nil {
				log.Printf("failed to destroy container %s: %v\n", busyCont.ID, err)
				continue
			}

			LocalResources.usedCPUs -= functionDescriptor.CPUDemand
			LocalResources.busyPoolUsedMem -= functionDescriptor.MemoryMB

			pool.busy[i] = nil
		}
		// Reset the busy slice capacity
		pool.busy = pool.busy[:0]
	}
}

// WarmStatus foreach function returns the corresponding number of warm container available
func WarmStatus() map[string]int {
	LocalResources.RLock()
	defer LocalResources.RUnlock()
	warmPool := make(map[string]int)
	for funcName, pool := range LocalResources.containerPools {
		warmPool[funcName] = len(pool.idle)
	}

	return warmPool
}

func PrewarmInstances(f *function.Function, count int64, forcePull bool) (int64, error) {
	var spawned int64 = 0
	for spawned < count {
		if !AcquireResourcesForNewContainer(f, true) {
			//log.Printf("Not enough resources for the new container.\n")
			return spawned, OutOfResourcesErr
		}

		_, err := NewContainerWithAcquiredResources(f, true, forcePull)
		if err != nil {
			log.Printf("Prespawning failed: %v\n", err)
			return spawned, err
		}
		spawned += 1
		forcePull = false // not needed more than once
	}

	return spawned, nil
}
