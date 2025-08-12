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
	IPAddress string
	RemoteURL string
}

type StatusInformation struct {
	Url                     string
	AvailableWarmContainers map[string]int // <k, v> = <function name, warm container number>
	AvailableMemMB          int64
	AvailableCPUs           float64
	Coordinates             vivaldi.Coordinate
	LoadAvg                 []float64
}
