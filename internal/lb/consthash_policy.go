package lb

import (
	"crypto/sha256"
	"errors"
	"github.com/serverledge-faas/serverledge/internal/function"
	"github.com/serverledge-faas/serverledge/internal/registration"
	"log"
	"sort"
	"sync"
)

type constHashBalancer struct {
	ring        *Ring
	availMemory map[string]int64
	mutex       sync.RWMutex
}

func (c *constHashBalancer) OnRequestComplete(funcName string, nodeKey string) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	f, found := function.GetFunction(funcName)
	if !found {
		return
	}
	c.availMemory[nodeKey] += f.MemoryMB
}

type ringElem struct {
	key  uint64
	node string
}

// Ring is a structure that maintains nodes in sorted order
type Ring struct {
	nodes []ringElem
}

// Helper function to generate a key
func hash(key string) uint64 {
	hash := sha256.New()
	hash.Write([]byte(key))
	hashBytes := hash.Sum(nil)
	var hashValue uint64
	for _, b := range hashBytes[:8] { // Use only the first 8 bytes to get a uint64
		hashValue = hashValue<<8 + uint64(b)
	}
	return hashValue
}

// Adds a node to the ring
func (r *Ring) addNode(node *registration.NodeRegistration) {
	key := hash(node.APIUrl())
	r.nodes = append(r.nodes, ringElem{key: key, node: node.Key})
	sort.Slice(r.nodes, func(i, j int) bool {
		return r.nodes[i].key < r.nodes[j].key
	})
}

func newConstHashBalancer() *constHashBalancer {
	ring := &Ring{}
	ring.nodes = make([]ringElem, 0)

	for _, node := range currentTargets {
		ring.addNode(&node)
	}

	c := &constHashBalancer{ring: ring}
	c.availMemory = make(map[string]int64)
	return c
}

func (c *constHashBalancer) Route(funcName string) (string, error) {
	f, ok := function.GetFunction(funcName)
	if !ok {
		return "", errors.New("function not found")
	}
	key := hash(funcName)

	c.mutex.Lock()
	defer c.mutex.Unlock()

	// Find the index where the key should be inserted
	startIndex := sort.Search(len(c.ring.nodes), func(i int) bool {
		return c.ring.nodes[i].key > key
	})

	if startIndex >= len(c.ring.nodes) {
		startIndex = 0
	}

	// Check nodes starting from the startIndex and wrap around if necessary
	for _, entry := range append(c.ring.nodes[startIndex:], c.ring.nodes[:startIndex]...) {
		nodeKey := entry.node
		availMemory, ok := c.availMemory[nodeKey]
		if ok && availMemory >= f.MemoryMB {
			//log.Printf("Using %s for function %s", nodeKey, funcName)
			c.availMemory[nodeKey] -= f.MemoryMB // TODO: inaccurate, if a warm container is available
			return nodeKey, nil
		} else if !ok {
			log.Printf("WARNING: no info about available memory for node %s", nodeKey)
		}
	}

	return "", errors.New("no available target")
}
func (c *constHashBalancer) OnNodeArrival(registration *registration.NodeRegistration) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	c.ring.addNode(registration)
}

func (c *constHashBalancer) OnNodeDeletion(registration *registration.NodeRegistration) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	toDelete := -1
	for i, n := range c.ring.nodes {
		if n.node == registration.Key {
			toDelete = i
			break
		}
	}

	if toDelete != -1 {
		c.ring.nodes = append(c.ring.nodes[:toDelete], c.ring.nodes[toDelete+1:]...)
		delete(c.availMemory, registration.Key)
	}
}

func (c *constHashBalancer) OnStatusUpdate(registration *registration.NodeRegistration, status registration.StatusInformation) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	c.availMemory[registration.Key] = status.TotalMemory - status.UsedMemory
}
