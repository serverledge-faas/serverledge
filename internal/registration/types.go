package registration

import (
	"errors"

	"github.com/LK4D4/trylock"
	"github.com/hexablock/vivaldi"
)

var UnavailableClientErr = errors.New("etcd client unavailable")
var IdRegistrationErr = errors.New("etcd error: could not complete the registration")
var KeepAliveErr = errors.New(" The system can't renew your registration key")

type Registry struct {
	Area             string
	Key              string
	Client           *vivaldi.Client
	RwMtx            trylock.Mutex
	NearbyServersMap map[string]*StatusInformation
	serversMap       map[string]*StatusInformation
	etcdCh           chan bool
}

type StatusInformation struct {
	Url                     string
	AvailableWarmContainers map[string]int // <k, v> = <function name, warm container number>
	AvailableMemMB          int64
	AvailableCPUs           float64
	Coordinates             vivaldi.Coordinate
	LoadAvg                 []float64
}
