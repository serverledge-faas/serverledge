package node

import (
	"container/list"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/serverledge-faas/serverledge/internal/config"
	"github.com/serverledge-faas/serverledge/internal/container"
	"github.com/serverledge-faas/serverledge/internal/function"
)

type ContainerPool struct {
	busy *list.List
	idle *list.List
}

var NoWarmFoundErr = errors.New("no warm container is available")

// GetContainerPool retrieves (or creates) the container pool for a function.
func GetContainerPool(f *function.Function) *ContainerPool {
	if fp, ok := Resources.ContainerPools[f.Name]; ok {
		return fp
	}

	fp := newContainerPool()
	Resources.ContainerPools[f.Name] = fp
	return fp
}

func (fp *ContainerPool) popIdleContainer() (*container.Container, bool) {
	// TODO: picking most-recent / least-recent container might be better?
	elem := fp.idle.Front()
	if elem == nil {
		return nil, false
	}

	c := fp.idle.Remove(elem).(*container.Container)

	return c, true
}

func (fp *ContainerPool) getReusableContainer(maxConcurrency int16) (*container.Container, bool) {
	for elem := fp.busy.Front(); elem != nil; elem = elem.Next() {
		c := elem.Value.(*container.Container)
		if c.RequestsCount < maxConcurrency {
			return c, true
		}
	}

	return nil, false
}

func newContainerPool() *ContainerPool {
	fp := &ContainerPool{}
	fp.busy = list.New()
	fp.idle = list.New()

	return fp
}

// AcquireResources reserves the specified amount of cpu and memory if possible.
func AcquireResources(cpuDemand float64, memDemand int64, destroyContainersIfNeeded bool) bool {
	Resources.Lock()
	defer Resources.Unlock()
	return acquireResources(cpuDemand, memDemand, destroyContainersIfNeeded)
}

// acquireResources reserves the specified amount of cpu and memory if possible.
// The function is NOT thread-safe.
func acquireResources(cpuDemand float64, memDemand int64, destroyContainersIfNeeded bool) bool {
	if Resources.AvailableCPUs < cpuDemand {
		return false
	}
	if Resources.AvailableMemMB < memDemand {
		if !destroyContainersIfNeeded {
			return false
		}

		enoughMem, _ := dismissContainer(memDemand)
		if !enoughMem {
			return false
		}
	}

	Resources.AvailableCPUs -= cpuDemand
	Resources.AvailableMemMB -= memDemand

	return true
}

// releaseResources releases the specified amount of cpu and memory.
// The function is NOT thread-safe.
func releaseResources(cpuDemand float64, memDemand int64) {
	Resources.AvailableCPUs += cpuDemand
	Resources.AvailableMemMB += memDemand
}

// AcquireWarmContainer acquires a warm container for a given function (if any).
// A warm container is in running/paused state and has already been initialized
// with the function code.
// The function returns an error if either:
// (i) the warm container does not exist
// (ii) there are not enough resources to start the container
func AcquireWarmContainer(f *function.Function) (*container.Container, error) {
	Resources.Lock()
	defer Resources.Unlock()

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

	if !acquireResources(f.CPUDemand, 0, false) {
		//log.Printf("Not enough CPU to start a warm container for %s", f)
		return nil, OutOfResourcesErr
	}

	// add container to the busy pool
	c.RequestsCount = 1
	fp.busy.PushBack(c)

	log.Printf("Using warm %s for %s. Now: %v", c.ID, f, Resources)
	return c, nil
}

func AcquireContainer(f *function.Function) (*container.Container, bool, error) {
	c, err := AcquireWarmContainer(f)
	if err == nil {
		return c, true, nil
	} else if !errors.Is(err, NoWarmFoundErr) {
		return nil, true, err
	}

	// Cold start required
	if !AcquireResources(f.CPUDemand, f.MemoryMB, true) {
		return nil, false, OutOfResourcesErr
	}
	c, err = NewContainer(f, false)
	return c, false, err
}

func HandleCompletion(cont *container.Container, f *function.Function) {
	Resources.Lock()
	defer Resources.Unlock()

	cont.RequestsCount--
	if cont.RequestsCount == 0 {
		// the container is now idle and must be moved to the warm pool
		fp := GetContainerPool(f)
		// we must update the busy list by removing this element
		var deleted interface{}
		elem := fp.busy.Front()
		for ok := elem != nil; ok; ok = elem != nil {
			if elem.Value.(*container.Container) == cont {
				deleted = fp.busy.Remove(elem) // delete the element from the busy list
				break
			}
			elem = elem.Next()
		}
		if deleted == nil {
			log.Println("Failed to release a container!")
			return
		}

		d := time.Duration(config.GetInt(config.CONTAINER_EXPIRATION_TIME, 600)) * time.Second
		cont.ExpirationTime = time.Now().Add(d).UnixNano()
		fp.idle.PushBack(cont)
		releaseResources(f.CPUDemand, 0)
	}
}

// NewContainer creates and starts a new container for the given function.
// The container can be directly used to schedule a request, as it is already
// in the busy pool.
func NewContainer(fun *function.Function, markAsIdle bool) (*container.Container, error) {
	Resources.Lock()
	if !acquireResources(fun.CPUDemand, fun.MemoryMB, true) {
		//log.Printf("Not enough resources for the new container.\n")
		Resources.Unlock()
		return nil, OutOfResourcesErr
	}

	//log.Printf("Acquired resources for new container. Now: %v", Resources)
	Resources.Unlock()

	return NewContainerWithAcquiredResources(fun, markAsIdle)
}

func getImageForFunction(fun *function.Function) (string, error) {
	var image string
	if fun.Runtime == container.CUSTOM_RUNTIME {
		image = fun.CustomImage
	} else {
		runtime, ok := container.RuntimeToInfo[fun.Runtime]
		if !ok {
			log.Printf("Unknown runtime: %s\n", fun.Runtime)
			return "", fmt.Errorf("Invalid runtime: %s", fun.Runtime)
		}
		image = runtime.Image
	}
	return image, nil
}

// NewContainerWithAcquiredResources spawns a new container for the given
// function, assuming that the required CPU and memory resources have been
// already been acquired.
func NewContainerWithAcquiredResources(fun *function.Function, startAsIdle bool) (*container.Container, error) {
	image, err := getImageForFunction(fun)
	if err != nil {
		return nil, err
	}

	cont, err := container.NewContainer(image, fun.TarFunctionCode, &container.ContainerOptions{
		MemoryMB: fun.MemoryMB,
		CPUQuota: fun.CPUDemand,
	})

	if err != nil {
		log.Printf("Failed container creation: %v\n", err)
	}

	Resources.Lock()
	defer Resources.Unlock()
	if err != nil {
		releaseResources(fun.CPUDemand, fun.MemoryMB)
		return nil, err
	}

	fp := GetContainerPool(fun)
	if startAsIdle {
		fp.idle.PushBack(cont)
	} else {
		cont.RequestsCount = 1
		fp.busy.PushBack(cont) // We immediately mark it as busy
	}

	return cont, nil
}

type itemToDismiss struct {
	contID container.ContainerID
	pool   *ContainerPool
	elem   *list.Element
	memory int64
}

// dismissContainer ... this function is used to get free memory used for a new container
// 2-phases: first, we find idle container and collect them as a slice, second (cleanup phase) we delete the container only and only if
// the sum of their memory is >= requiredMemoryMB is
func dismissContainer(requiredMemoryMB int64) (bool, error) {
	var cleanedMB int64 = 0
	var containerToDismiss []itemToDismiss
	res := false

	//first phase, research
	for _, funPool := range Resources.ContainerPools {
		if funPool.idle.Len() > 0 {
			// every container into the funPool has the same memory (same function)
			//so it is not important which one you destroy
			elem := funPool.idle.Front()
			contID := elem.Value.(*container.Container).ID
			// container in the same pool need same memory
			memory, _ := container.GetMemoryMB(contID)
			for ok := true; ok; ok = elem != nil {
				containerToDismiss = append(containerToDismiss,
					itemToDismiss{contID: contID, pool: funPool, elem: elem, memory: memory})
				cleanedMB += memory
				if cleanedMB >= requiredMemoryMB {
					goto cleanup
				}
				//go on to the next one
				elem = elem.Next()
			}
		}
	}

cleanup: // second phase, cleanup
	// memory check
	if cleanedMB >= requiredMemoryMB {
		for _, item := range containerToDismiss {
			item.pool.idle.Remove(item.elem)      // remove the container from the funPool
			err := container.Destroy(item.contID) // destroy the container
			if err != nil {
				res = false
				return res, nil
			}
			Resources.AvailableMemMB += item.memory
		}

		res = true
	}
	return res, nil
}

// DeleteExpiredContainer is called by the container cleaner
// Deletes expired warm container
func DeleteExpiredContainer() {
	now := time.Now().UnixNano()

	Resources.Lock()
	defer Resources.Unlock()

	for _, pool := range Resources.ContainerPools {
		elem := pool.idle.Front()
		for ok := elem != nil; ok; ok = elem != nil {
			warm := elem.Value.(*container.Container)
			if now > warm.ExpirationTime {
				temp := elem
				elem = elem.Next()
				//log.Printf("cleaner: Removing container %s\n", warm.contID)
				pool.idle.Remove(temp) // remove the expired element

				memory, _ := container.GetMemoryMB(warm.ID)
				releaseResources(0, memory)
				err := container.Destroy(warm.ID)
				if err != nil {
					log.Printf("Error while destroying container %s: %s\n", warm.ID, err)
				}
				// log.Printf("Released resources. Now: %v\n", &Resources)
			} else {
				elem = elem.Next()
			}
		}
	}

}

// ShutdownWarmContainersFor destroys warm containers of a given function
// Actual termination happens asynchronously.
func ShutdownWarmContainersFor(f *function.Function) {
	Resources.Lock()
	defer Resources.Unlock()

	fp, ok := Resources.ContainerPools[f.Name]
	if !ok {
		return
	}

	containersToDelete := make([]container.ContainerID, 0)

	elem := fp.idle.Front()
	for ok := elem != nil; ok; ok = elem != nil {
		warmed := elem.Value.(*container.Container)
		temp := elem
		elem = elem.Next()
		log.Printf("Removing container with ID %s\n", warmed.ID)
		fp.idle.Remove(temp)

		memory, _ := container.GetMemoryMB(warmed.ID)
		Resources.AvailableMemMB += memory
		containersToDelete = append(containersToDelete, warmed.ID)
	}

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
	Resources.Lock()
	defer Resources.Unlock()

	for fun, pool := range Resources.ContainerPools {
		for elem := pool.idle.Front(); elem != nil; elem = elem.Next() {
			warmed := elem.Value.(*container.Container)
			temp := elem
			log.Printf("Removing container with ID %s\n", warmed.ID)
			pool.idle.Remove(temp)

			memory, _ := container.GetMemoryMB(warmed.ID)
			err := container.Destroy(warmed.ID)
			if err != nil {
				log.Printf("Error while destroying container %s: %s", warmed.ID, err)
			}
			Resources.AvailableMemMB += memory
		}

		functionDescriptor, _ := function.GetFunction(fun)

		for elem := pool.idle.Front(); elem != nil; elem = elem.Next() {
			contID := elem.Value.(*container.Container).ID
			temp := elem
			log.Printf("Removing container with ID %s\n", contID)
			pool.idle.Remove(temp)

			memory, errMem := container.GetMemoryMB(contID)
			if errMem != nil {
				log.Printf("failed to get memory from container %s before destroying it: %v", contID, errMem)
				continue
			}
			err := container.Destroy(contID)
			if err != nil {
				log.Printf("failed to destroy container %s: %v\n", contID, err)
				continue
			}
			Resources.AvailableMemMB += memory
			Resources.AvailableCPUs += functionDescriptor.CPUDemand
		}
	}
}

// WarmStatus foreach function returns the corresponding number of warm container available
func WarmStatus() map[string]int {
	Resources.RLock()
	defer Resources.RUnlock()
	warmPool := make(map[string]int)
	for funcName, pool := range Resources.ContainerPools {
		warmPool[funcName] = pool.idle.Len()
	}

	return warmPool
}

func PrewarmInstances(f *function.Function, count int64, forcePull bool) (int64, error) {
	image, err := getImageForFunction(f)
	if err != nil {
		return 0, err
	}
	err = container.DownloadImage(image, forcePull)
	if err != nil {
		return 0, err
	}

	var spawned int64 = 0
	for spawned < count {
		_, err = NewContainer(f, true)
		if err != nil {
			log.Printf("Prespawning failed: %v\n", err)
			return spawned, err
		}
		spawned += 1
	}

	return spawned, nil
}
