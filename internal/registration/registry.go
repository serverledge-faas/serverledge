package registration

import (
	"fmt"
	"github.com/hexablock/vivaldi"
	"github.com/serverledge-faas/serverledge/internal/node"
	"golang.org/x/exp/maps"
	"log"
	"net"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/serverledge-faas/serverledge/internal/config"
	"github.com/serverledge-faas/serverledge/utils"
	"go.etcd.io/etcd/client/v3"
	"golang.org/x/net/context"
)

const registryBaseDirectory = "registry"
const registryLoadBalancerDirectory = "__lb"
const etcdLeaseTTL = 120

var mutex sync.RWMutex

var nearestNeighbors []NodeRegistration
var neighborInfo map[string]*StatusInformation
var neighbors map[string]NodeRegistration

var remoteOffloadingTarget NodeRegistration
var remoteOffloadingTargetLatencyMs float64

var VivaldiClient *vivaldi.Client
var SelfRegistration *NodeRegistration

var etcdClient *clientv3.Client = nil
var etcdLease clientv3.LeaseID

func (r *NodeRegistration) toEtcdKey() (key string) {
	if r.IsLoadBalancer {
		return fmt.Sprintf("%s/%s/%s/%s", registryBaseDirectory, r.Area, registryLoadBalancerDirectory, r.Key)
	} else {
		return fmt.Sprintf("%s/%s/%s", registryBaseDirectory, r.Area, r.Key)
	}
}

func (r *NodeRegistration) APIUrl() (url string) {
	return fmt.Sprintf("http://%s:%d", r.IPAddress, r.APIPort)
}

func areaEtcdKey(area string) string {
	return fmt.Sprintf("%s/%s/", registryBaseDirectory, area)
}

// RegisterNode make a registration to the local Area
func registerToEtcd(asLoadBalancer bool) error {
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

	ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
	resp, err := etcdClient.Grant(ctx, etcdLeaseTTL)
	if err != nil {
		log.Fatalf("Could not grant lease: %v", err)
		return err
	}
	etcdLease = resp.ID

	registeredLocalIP := config.GetString(config.API_IP, defaultAddressStr)
	apiPort := config.GetInt(config.API_PORT, 1323)
	udpPort := config.GetInt(config.LISTEN_UDP_PORT, 9876)

	payload := fmt.Sprintf("%s;%d;%d", registeredLocalIP, apiPort, udpPort)

	SelfRegistration = &NodeRegistration{NodeID: node.LocalNode, IPAddress: registeredLocalIP, APIPort: apiPort, UDPPort: udpPort, IsLoadBalancer: asLoadBalancer}

	// save couple (id, hostport) to the correct Area-dir on etcd
	etcdKey := SelfRegistration.toEtcdKey()
	log.Printf("Registering to etcd: %s\n", etcdKey)
	_, err = etcdClient.Put(ctx, etcdKey, payload, clientv3.WithLease(etcdLease))
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

// RegisterNode make a registration to the local Area
func RegisterNode() error {
	return registerToEtcd(false)
}

func RegisterLoadBalancer() error {
	return registerToEtcd(true)
}

func keepAliveLease() {
	_, err := etcdClient.KeepAliveOnce(context.Background(), etcdLease)
	if err != nil {
		log.Printf("Error keeping alive lease: %v", err)
	}
}

func parseEtcdRegisteredNode(area string, key string, payload []byte) (NodeRegistration, error) {
	payloadStr := string(payload)
	split := strings.Split(payloadStr, ";")
	if len(split) < 3 {
		return NodeRegistration{}, fmt.Errorf("invalid payload: %s", payloadStr)
	}

	ipAddress := split[0]

	apiPort, err := strconv.Atoi(split[1])
	if err != nil {
		return NodeRegistration{}, err
	}

	udpPort, err := strconv.Atoi(split[2])
	if err != nil {
		return NodeRegistration{}, err
	}

	return NodeRegistration{NodeID: node.NodeID{Area: area, Key: key}, IPAddress: ipAddress, APIPort: apiPort, UDPPort: udpPort}, nil
}

// GetNodesInArea is used to obtain the list of  other server's addresses under a specific local Area
func GetNodesInArea(area string, includeSelf bool, limit int64) (map[string]NodeRegistration, error) {
	baseDir := areaEtcdKey(area)
	lbPrefix := path.Join(baseDir, registryLoadBalancerDirectory)

	ctx, _ := context.WithTimeout(context.Background(), 3*time.Second)

	if limit < 0 {
		limit = 0 // no limit
	}
	resp, err := etcdClient.Get(ctx, baseDir, clientv3.WithPrefix(), clientv3.WithLimit(limit))
	if err != nil {
		return nil, fmt.Errorf("Could not read from etcd: %v", err)
	}

	servers := make(map[string]NodeRegistration)
	for _, s := range resp.Kvs {
		if strings.HasPrefix(string(s.Key), lbPrefix) {
			// skip LB
			continue
		}
		key := path.Base(string(s.Key))
		if !includeSelf && area == SelfRegistration.Area && key == SelfRegistration.Key {
			continue
		}

		reg, err := parseEtcdRegisteredNode(area, key, s.Value)
		if err == nil {
			servers[key] = reg
			fmt.Printf("Server found: %v\n", servers[key])
		}
	}

	return servers, nil
}

func GetOneNodeInArea(area string, includeSelf bool) (NodeRegistration, error) {
	nodes, err := GetNodesInArea(area, includeSelf, 1)
	if err == nil {
		for _, n := range nodes {
			return n, nil
		}
		return NodeRegistration{}, fmt.Errorf("no nodes found")
	}

	return NodeRegistration{}, err
}

func GetLBInArea(area string) (map[string]NodeRegistration, error) {
	baseDir := areaEtcdKey(area) + "/" + registryLoadBalancerDirectory

	ctx, _ := context.WithTimeout(context.Background(), 3*time.Second)

	resp, err := etcdClient.Get(ctx, baseDir, clientv3.WithPrefix())
	if err != nil {
		return nil, fmt.Errorf("Could not read from etcd: %v", err)
	}

	servers := make(map[string]NodeRegistration)
	for _, s := range resp.Kvs {
		key := path.Base(string(s.Key))
		reg, err := parseEtcdRegisteredNode(area, key, s.Value)
		if err == nil {
			reg.IsLoadBalancer = true
			servers[key] = reg
			fmt.Printf("Server found: %v\n", servers[key])
		}
	}

	return servers, nil
}

func Deregister() error {
	ctx, _ := context.WithTimeout(context.Background(), 1*time.Second)
	_, err := etcdClient.Revoke(ctx, etcdLease)
	if err != nil {
		log.Printf("Error revoking lease: %v", err)
	}

	return nil
}

func StartMonitoring() error {

	neighbors = make(map[string]NodeRegistration)
	neighborInfo = make(map[string]*StatusInformation)

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
	newNeighbors, err := GetNodesInArea(SelfRegistration.Area, false, 0)
	if err != nil {
		log.Println(err)
		return
	}

	mutex.Lock()
	defer mutex.Unlock()

	neighbors = newNeighbors

	//deletes information about servers that haven't registered anymore
	for key := range neighborInfo {
		_, ok := neighbors[key]
		if !ok {
			delete(neighborInfo, key)
		}
	}

	updateRemoteOffloadingTarget()
	updateLatencyToOffloadingTarget()
}

func updateLatencyToOffloadingTarget() {
	if remoteOffloadingTarget.Key == "" {
		remoteOffloadingTargetLatencyMs = 9999.0
		return
	}

	hostAndPort := fmt.Sprintf("%s:%d", remoteOffloadingTarget.IPAddress, remoteOffloadingTarget.APIPort)
	latency, err := tcpLatency(hostAndPort)
	if err != nil {
		log.Println(err)
	} else {
		log.Printf("Latency for remote offloading target is %v", latency)
		remoteOffloadingTargetLatencyMs = latency
	}
}

func tcpLatency(hostAndPort string) (float64, error) {
	start := time.Now()
	conn, err := net.DialTimeout("tcp", hostAndPort, 3*time.Second)
	if err != nil {
		return 0, err
	}
	_ = conn.Close()
	return float64(time.Since(start).Milliseconds()), nil
}

func updateRemoteOffloadingTarget() {
	// If there is a LB in the remote area, it is used.
	// Otherwise, a random node in the area is chosen.

	remoteArea := config.GetString(config.REGISTRY_REMOTE_AREA, "")
	if remoteArea == "" {
		log.Printf("No remote area is configured; vertical offloading disabled")
		remoteOffloadingTarget = NodeRegistration{}
		return
	}

	lbs, err := GetLBInArea(remoteArea)
	if err != nil {
		log.Println(err)
	}
	if err == nil && len(lbs) > 0 {
		for _, lb := range lbs {
			log.Printf("Using LB as offloading target: %v", lb.NodeID)
			remoteOffloadingTarget = lb
			return
		}
	}

	remoteNode, err := GetOneNodeInArea(remoteArea, false)
	if err == nil {
		log.Printf("Using as offloading target: %v", remoteNode.NodeID)
		remoteOffloadingTarget = remoteNode
	}
}

// computeNearestNeighbors finds servers nearby to the current one
func computeNearestNeighbors(nNeighbors int) {
	type dist struct {
		key      string
		distance time.Duration
	}

	if nNeighbors > len(neighborInfo) {
		nearestNeighbors = make([]NodeRegistration, 0, len(neighborInfo))
		for _, n := range neighbors {
			nearestNeighbors = append(nearestNeighbors, n)
		}
		return
	}

	var distanceBuf = make([]dist, len(neighborInfo)) //distances from current server
	for key, s := range neighborInfo {
		distanceBuf = append(distanceBuf, dist{key, VivaldiClient.DistanceTo(&s.Coordinates)})
	}
	sort.Slice(distanceBuf, func(i, j int) bool { return distanceBuf[i].distance < distanceBuf[j].distance })

	nearestNeighbors = make([]NodeRegistration, nNeighbors)
	for i := 0; i < nNeighbors; i++ {
		k := distanceBuf[i].key
		nearestNeighbors[i] = neighbors[k]
	}
}

// nearbyMonitoring check nearby server's status
func nearbyMonitoring(vivaldiClient *vivaldi.Client) {
	log.Printf("Periodic nearby Monitoring\n")

	mutex.RLock()
	// TODO: randomly choose a subset of peers for update?
	peersToUpdate := make([]NodeRegistration, len(neighbors))
	for _, reg := range neighbors {
		peersToUpdate = append(peersToUpdate, reg)
	}
	mutex.RUnlock()

	for _, registeredNode := range peersToUpdate {
		newInfo, rtt := statusInfoRequest(&registeredNode)

		if newInfo == nil {
			log.Printf("Unreachable neighbor: %s\n", registeredNode.NodeID)
			continue
		}

		mutex.Lock()
		neighborInfo[registeredNode.Key] = newInfo

		_, err := vivaldiClient.Update("node", &newInfo.Coordinates, rtt)
		if err != nil {
			log.Printf("Error while updating node coordinates: %s\n", err)
		}
		mutex.Unlock()
	}

	// Updates neighborInfo with the N closest nodes from serverMap
	computeNearestNeighbors(2) //todo change this value
}

func GetNearestNeighbors() []NodeRegistration {
	return nearestNeighbors
}

func GetRemoteOffloadingTarget() *NodeRegistration {
	if remoteOffloadingTarget.Key != "" {
		return &remoteOffloadingTarget
	}
	return nil
}

func GetFullNeighborInfo() map[string]*StatusInformation {
	mutex.RLock()
	defer mutex.RUnlock()
	return maps.Clone(neighborInfo)
}
