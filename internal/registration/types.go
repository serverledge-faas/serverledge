package registration

import (
	"errors"
	"github.com/hexablock/vivaldi"
	"github.com/serverledge-faas/serverledge/internal/node"
)

var UnavailableClientErr = errors.New("etcd client unavailable")
var IdRegistrationErr = errors.New("etcd error: could not complete the registration")

type NodeRegistration struct {
	node.NodeID
	IPAddress      string
	APIPort        int
	UDPPort        int
	IsLoadBalancer bool
}

type StatusInformation struct {
	AvailableWarmContainers map[string]int // <k, v> = <function name, warm container number>
	TotalMemory             int64
	AvailableMemory         int64
	FreeMemory              int64
	TotalCPU                float64
	UsedCPU                 float64
	Coordinates             vivaldi.Coordinate
	LoadAvg                 []float64
}
