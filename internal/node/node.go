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
	return fmt.Sprintf("%s/%s", n.Area, n.Key)
}

func NewIdentifier(area string) NodeID {
	id := shortuuid.New() + strconv.FormatInt(time.Now().UnixNano(), 10)
	return NodeID{Area: area, Key: id}
}

type NodeResources struct {
	sync.RWMutex
	AvailableMemMB int64
	AvailableCPUs  float64
	DropCount      int64
	ContainerPools map[string]*ContainerPool
}

func (n *NodeResources) String() string {
	return fmt.Sprintf("[CPUs: %f - Mem: %d]", n.AvailableCPUs, n.AvailableMemMB)
}

var Resources NodeResources
