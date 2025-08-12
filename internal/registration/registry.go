package registration

import (
	"fmt"
	"github.com/hexablock/vivaldi"
	"github.com/serverledge-faas/serverledge/internal/node"
	"log"
	"net/url"
	"sort"
	"sync"
	"time"

	"github.com/serverledge-faas/serverledge/internal/config"
	"github.com/serverledge-faas/serverledge/utils"
	"go.etcd.io/etcd/client/v3"
	"golang.org/x/net/context"
)

const registryBaseDirectory = "registry"
const etcdLeaseTTL = 120

var mutex sync.Mutex

var NearestNeighbors []string
var NeighborInfo map[string]*StatusInformation
var neighbors map[string]NodeRegistration
var VivaldiClient *vivaldi.Client
var SelfRegistration *NodeRegistration

var etcdClient *clientv3.Client = nil
var etcdLease clientv3.LeaseID

func (r *NodeRegistration) toEtcdKey() (key string) {
	return fmt.Sprintf("%s/%s/%s", registryBaseDirectory, r.Area, r.Key)
}

func areaEtcdKey(area string) string {
	return fmt.Sprintf("%s/%s/", registryBaseDirectory, area)
}

// RegisterToEtcd make a registration to the local Area
func RegisterToEtcd() error {
	log.Printf("Registration for node: %s\n", node.LocalNode)

	defaultAddressStr := "127.0.0.1"
	address, err := utils.GetOutboundIp()
	if err == nil {
		defaultAddressStr = address.String()
	}

	etcdClient, err = utils.GetEtcdClient()
	if err != nil {
		log.Fatal(UnavailableClientErr)
		return UnavailableClientErr
	}

	ctx, _ := context.WithTimeout(context.Background(), 1*time.Second)
	resp, err := etcdClient.Grant(ctx, etcdLeaseTTL)
	if err != nil {
		log.Fatal(err)
		return err
	}
	etcdLease = resp.ID

	registeredLocalIP := config.GetString(config.API_IP, defaultAddressStr)
	hostport := fmt.Sprintf("http://%s:%d", registeredLocalIP, config.GetInt(config.API_PORT, 1323))
	SelfRegistration := &NodeRegistration{NodeID: node.LocalNode, IPAddress: registeredLocalIP, RemoteURL: hostport}

	// save couple (id, hostport) to the correct Area-dir on etcd
	_, err = etcdClient.Put(ctx, SelfRegistration.toEtcdKey(), hostport, clientv3.WithLease(etcdLease))
	if err != nil {
		log.Fatal(IdRegistrationErr)
		return IdRegistrationErr
	}

	go func() {
		interval := time.Duration(float64(etcdLeaseTTL) * 0.7 * float64(time.Second))
		time.Sleep(interval)
		keepAliveLease()
	}()

	return nil
}

func keepAliveLease() {
	_, err := etcdClient.KeepAliveOnce(context.Background(), etcdLease)
	if err != nil {
		log.Printf("Error keeping alive lease: %v", err)
	}
}

// GetAllInArea is used to obtain the list of  other server's addresses under a specific local Area
func GetAllInArea(area string, includeSelf bool) (map[string]NodeRegistration, error) {
	baseDir := areaEtcdKey(area)

	ctx, _ := context.WithTimeout(context.Background(), 3*time.Second)

	resp, err := etcdClient.Get(ctx, baseDir, clientv3.WithPrefix())
	if err != nil {
		return nil, fmt.Errorf("Could not read from etcd: %v", err)
	}

	servers := make(map[string]NodeRegistration)
	for _, s := range resp.Kvs {
		remoteURL := string(s.Value)
		key := string(s.Key)
		if !includeSelf && area == SelfRegistration.Area && key == SelfRegistration.Key {
			continue
		}
		servers[key] = NodeRegistration{NodeID: node.NodeID{Area: area, Key: key}, RemoteURL: remoteURL}
		fmt.Printf("Server found: %v\n", servers[key])
	}

	return servers, nil
}

// Deregister deletes from etcd the key, value pair previously inserted
func Deregister() error {
	etcdClient, err := utils.GetEtcdClient()
	if err != nil {
		log.Fatal(UnavailableClientErr)
		return UnavailableClientErr
	}

	ctx, _ := context.WithTimeout(context.Background(), 1*time.Second)
	_, err = etcdClient.Revoke(ctx, etcdLease)
	if err != nil {
		return err
	}

	return nil
}
func StartMonitoring() error {

	neighbors = make(map[string]NodeRegistration)
	NeighborInfo = make(map[string]*StatusInformation)

	defaultConfig := vivaldi.DefaultConfig()
	defaultConfig.Dimensionality = 3
	var err error
	VivaldiClient, err = vivaldi.NewClient(defaultConfig)
	if err != nil {
		return err
	}

	//complete globalMonitoring phase at startup
	globalMonitoring()

	// start listening for incoming udp connections; use case: edge-nodes request for status infos
	go UDPStatusServer()
	go runMonitor()

	return nil
}

func runMonitor() {
	nearbyTicker := time.NewTicker(time.Duration(config.GetInt(config.REG_NEARBY_INTERVAL, 20)) * time.Second)         //wake-up nearby globalMonitoring
	monitoringTicker := time.NewTicker(time.Duration(config.GetInt(config.REG_MONITORING_INTERVAL, 30)) * time.Second) // wake-up general-area globalMonitoring
	for {
		select {
		case <-monitoringTicker.C:
			globalMonitoring()
		case <-nearbyTicker.C:
			nearbyMonitoring(VivaldiClient)
		}
	}
}

func globalMonitoring() {

	// gets info from Etcd about other nodes in the area
	newNeighbors, err := GetAllInArea(SelfRegistration.Area, false)
	if err != nil {
		log.Println(err)
		return
	}

	mutex.Lock()
	defer mutex.Unlock()

	neighbors = newNeighbors

	//deletes information about servers that haven't registered anymore
	for key := range NeighborInfo {
		_, ok := neighbors[key]
		if !ok {
			delete(NeighborInfo, key)
		}
	}
}

// computeNearestNeighbors finds servers nearby to the current one
func computeNearestNeighbors(nNeighbors int) {
	type dist struct {
		key      string
		distance time.Duration
	}

	if nNeighbors > len(NeighborInfo) {
		NearestNeighbors = make([]string, 0, len(NeighborInfo))
		for k, _ := range neighbors {
			NearestNeighbors = append(NearestNeighbors, k)
		}
		return
	}

	var distanceBuf = make([]dist, len(NeighborInfo)) //distances from current server
	for key, s := range NeighborInfo {
		distanceBuf = append(distanceBuf, dist{key, VivaldiClient.DistanceTo(&s.Coordinates)})
	}
	sort.Slice(distanceBuf, func(i, j int) bool { return distanceBuf[i].distance < distanceBuf[j].distance })

	NearestNeighbors = make([]string, nNeighbors)
	for i := 0; i < nNeighbors; i++ {
		k := distanceBuf[i].key
		NearestNeighbors[i] = k
	}
}

// nearbyMonitoring check nearby server's status
func nearbyMonitoring(vivaldiClient *vivaldi.Client) {
	log.Printf("Periodic nearby Monitoring\n")

	mutex.Lock()
	defer mutex.Unlock()

	// TODO: randomly choose a subset of peers for update?

	for key, registeredNode := range neighbors {
		var oldInfo *StatusInformation = nil
		oldInfo, _ = NeighborInfo[key]

		u, err := url.Parse(registeredNode.RemoteURL)
		if err != nil {
			panic(err)
		}
		hostname := u.Hostname()
		newInfo, rtt := statusInfoRequest(hostname)

		if newInfo == nil {
			log.Printf("Unreachable neighbor: %s\n", key)
			//unreachable server
			if oldInfo != nil {
				delete(NeighborInfo, key)
			}
			continue
		}

		NeighborInfo[key] = newInfo

		_, err = vivaldiClient.Update("node", &newInfo.Coordinates, rtt)
		if err != nil {
			log.Printf("Error while updating node coordinates: %s\n", err)
		}
	}
	// Updates NeighborInfo with the N closest nodes from serverMap
	computeNearestNeighbors(2) //todo change this value
}
