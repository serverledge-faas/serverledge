package node

import (
	"errors"
	"fmt"
	"github.com/lithammer/shortuuid"
	"github.com/serverledge-faas/serverledge/internal/config"
	"runtime"
	"strconv"
	"sync"
	"time"
)

var OutOfResourcesErr = errors.New("not enough resources for function execution")

type NodeID struct {
	Area string
	Key  string
}

var LocalNode NodeID

func (n NodeID) String() string {
	return fmt.Sprintf("(%s)%s", n.Area, n.Key)
}

func NewIdentifier(area string) NodeID {
	id := shortuuid.New() + strconv.FormatInt(time.Now().UnixNano(), 10)
	return NodeID{Area: area, Key: id}
}

type Resources struct {
	sync.RWMutex
	totalMemory     int64
	totalCPUs       float64
	busyPoolUsedMem int64   // amount of memory used by functions currently running
	warmPoolUsedMem int64   // amount of memory used by warm containers
	usedCPUs        float64 // number of CPU used by functions currently running
	containerPools  map[string]*ContainerPool
}

func (n *Resources) Init() {
	availableCores := runtime.NumCPU()
	n.totalCPUs = config.GetFloat(config.POOL_CPUS, float64(availableCores))
	n.totalMemory = int64(config.GetInt(config.POOL_MEMORY_MB, 1024))
	n.containerPools = make(map[string]*ContainerPool)
}

func (n *Resources) String() string {
	return fmt.Sprintf("[CPUs: %f/%f - Mem: %d(+%d warm)/%d]", n.usedCPUs, n.totalCPUs, n.busyPoolUsedMem, n.warmPoolUsedMem, n.totalMemory)
}

func (n *Resources) FreeMemory() int64 {
	return n.totalMemory - n.busyPoolUsedMem - n.warmPoolUsedMem
}

// AvailableMemory returns the amount of memory that is free or reclaimable from warm containers
func (n *Resources) AvailableMemory() int64 {
	return n.totalMemory - n.busyPoolUsedMem
}

func (n *Resources) AvailableCPUs() float64 {
	return n.totalCPUs - n.usedCPUs
}

func (n *Resources) UsedMemory() int64 {
	return n.busyPoolUsedMem
}

func (n *Resources) UsedCPUs() float64 {
	return n.usedCPUs
}

func (n *Resources) TotalCPUs() float64 {
	return n.totalCPUs
}

func (n *Resources) TotalMemory() int64 {
	return n.totalMemory
}

var LocalResources Resources
