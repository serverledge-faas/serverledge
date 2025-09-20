package node

import (
	"errors"
	"fmt"
	"github.com/lithammer/shortuuid"
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
	TotalMemory     int64
	TotalCPUs       float64
	BusyPoolUsedMem int64
	WarmPoolUsedMem int64
	UsedCPUs        float64
	ContainerPools  map[string]*ContainerPool
}

func (n *Resources) String() string {
	return fmt.Sprintf("[CPUs: %f/%f - Mem: %d(%d warm)/%d]", n.UsedCPUs, n.TotalCPUs, n.BusyPoolUsedMem, n.WarmPoolUsedMem, n.TotalMemory)
}

func (n *Resources) FreeMemory() int64 {
	return n.TotalMemory - n.BusyPoolUsedMem - n.WarmPoolUsedMem
}

// AvailableMemory returns amount of memory that is free or reclaimable from warm containers
func (n *Resources) AvailableMemory() int64 {
	return n.TotalMemory - n.BusyPoolUsedMem
}

func (n *Resources) AvailableCPUs() float64 {
	return n.TotalCPUs - n.UsedCPUs
}

var LocalResources Resources
