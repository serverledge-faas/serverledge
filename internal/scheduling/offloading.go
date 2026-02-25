package scheduling

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"time"

	"github.com/serverledge-faas/serverledge/internal/client"
	"github.com/serverledge-faas/serverledge/internal/config"
	"github.com/serverledge-faas/serverledge/internal/function"
	"github.com/serverledge-faas/serverledge/internal/node"
	"github.com/serverledge-faas/serverledge/internal/registration"
)

// Offloading choice caching
var offloadingCache = make(map[string]*registration.NodeRegistration)
var cacheExpiration = make(map[string]time.Time)
var CacheValidity = 60 * time.Second
var NoSuitableNode = errors.New("no node supporting the function's runtime found")
var NoNeighbors = errors.New("the list of neighbors is empty")

func pickEdgeNodeForOffloading(r *scheduledRequest) (url string, err error) {
	// check cache first
	cached, ok := offloadingCache[r.Fun.Name]
	if ok && time.Now().Before(cacheExpiration[r.Fun.Name]) {
		return cached.APIUrl(), nil
	}

	// select best node
	nearestNeighbors := registration.GetNearestNeighbors()
	if nearestNeighbors == nil {
		return "", NoNeighbors
	}

	neighborStatus := registration.GetFullNeighborInfo()

	var bestNode *registration.NodeRegistration
	maxMem := int64(0)

	for _, nodeReg := range nearestNeighbors {
		status, ok := neighborStatus[nodeReg.Key]
		if !ok {
			continue
		}
		availableMemory := status.TotalMemory - status.UsedMemory
		if r.Fun.SupportsArch(nodeReg.Arch) && availableMemory > maxMem {
			maxMem = availableMemory
			bestNode = &nodeReg
		}
	}

	if bestNode != nil {
		cacheValidityInt := config.GetInt(config.OFFLOADING_CACHE_VALIDITY, 60)
		CacheValidity = time.Duration(cacheValidityInt) * time.Second
		offloadingCache[r.Fun.Name] = bestNode
		cacheExpiration[r.Fun.Name] = time.Now().Add(CacheValidity)
		return bestNode.APIUrl(), nil
	}

	return "", NoSuitableNode
}

func Offload(r *scheduledRequest, serverUrl string) error {
	// Prepare request
	request := client.InvocationRequest{Params: r.Params, QoSClass: r.Class, QoSMaxRespT: r.MaxRespT, ReturnOutput: r.ReturnOutput}
	invocationBody, err := json.Marshal(request)
	if err != nil {
		log.Print(err)
		return err
	}
	sendingTime := time.Now() // used to compute latency later on
	resp, err := offloadingClient.Post(serverUrl+"/invoke/"+r.Fun.Name, "application/json",
		bytes.NewBuffer(invocationBody))

	if err != nil {
		log.Print(err)
		return err
	}
	if resp.StatusCode != http.StatusOK {
		if resp.StatusCode == http.StatusTooManyRequests {
			return node.OutOfResourcesErr
		}
		return fmt.Errorf("Remote returned: %v", resp.StatusCode)
	}

	var response function.Response
	defer func(Body io.ReadCloser) {
		err := Body.Close()
		if err != nil {
			fmt.Printf("Error while closing offload response body: %s\n", err)
		}
	}(resp.Body)
	body, _ := io.ReadAll(resp.Body)
	if err = json.Unmarshal(body, &response); err != nil {
		return err
	}
	now := time.Now()

	originalArrivalTime := r.Arrival
	r.ExecutionReport = &response.ExecutionReport // switching execution report
	r.ResponseTime = now.Sub(originalArrivalTime).Seconds()
	r.OffloadLatency = now.Sub(sendingTime).Seconds() - r.Duration - r.InitTime
	r.offloaded = true

	return nil
}

func OffloadAsync(r *function.Request, serverUrl string) error {
	// Prepare request
	request := client.InvocationRequest{Params: r.Params,
		QoSClass:    r.Class,
		QoSMaxRespT: r.MaxRespT,
		Async:       true}
	invocationBody, err := json.Marshal(request)
	if err != nil {
		log.Print(err)
		return err
	}
	resp, err := offloadingClient.Post(serverUrl+"/invoke/"+r.Fun.Name, "application/json",
		bytes.NewBuffer(invocationBody))

	if err != nil {
		log.Print(err)
		return err
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("Remote returned: %v", resp.StatusCode)
	}

	// there is nothing to wait for
	return nil
}
