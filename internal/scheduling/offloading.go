package scheduling

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"time"

	"github.com/serverledge-faas/serverledge/internal/client"
	"github.com/serverledge-faas/serverledge/internal/function"
	"github.com/serverledge-faas/serverledge/internal/node"
	"github.com/serverledge-faas/serverledge/internal/registration"
)

const SCHED_ACTION_OFFLOAD = "O"

func pickEdgeNodeForOffloading(r *scheduledRequest) (url string) {
	nearbyServersMap := registration.Reg.NearbyServersMap
	if nearbyServersMap == nil {
		return ""
	}
	//first, search for warm container
	for _, v := range nearbyServersMap {
		if v.AvailableWarmContainers[r.Fun.Name] != 0 && v.AvailableCPUs >= r.Request.Fun.CPUDemand {
			return v.Url
		}
	}
	//second, (nobody has warm container) search for available memory
	for _, v := range nearbyServersMap {
		if v.AvailableMemMB >= r.Request.Fun.MemoryMB && v.AvailableCPUs >= r.Request.Fun.CPUDemand {
			return v.Url
		}
	}
	return ""
}

func Offload(r *function.Request, serverUrl string) (function.ExecutionReport, error) {
	// Prepare request
	request := client.InvocationRequest{Params: r.Params, QoSClass: r.Class, QoSMaxRespT: r.MaxRespT}
	invocationBody, err := json.Marshal(request)
	if err != nil {
		log.Print(err)
		return function.ExecutionReport{}, err
	}
	sendingTime := time.Now() // used to compute latency later on
	resp, err := offloadingClient.Post(serverUrl+"/invoke/"+r.Fun.Name, "application/json",
		bytes.NewBuffer(invocationBody))

	if err != nil {
		log.Print(err)
		return function.ExecutionReport{}, err
	}
	if resp.StatusCode != http.StatusOK {
		if resp.StatusCode == http.StatusTooManyRequests {
			return function.ExecutionReport{}, node.OutOfResourcesErr
		}
		return function.ExecutionReport{}, fmt.Errorf("Remote returned: %v", resp.StatusCode)
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
		return function.ExecutionReport{}, err
	}
	now := time.Now()

	execReport := &response.ExecutionReport
	execReport.ResponseTime = now.Sub(r.Arrival).Seconds()

	// TODO: check how this is used in the QoSAware policy
	// It was originially computed as "report.Arrival - sendingTime"
	execReport.OffloadLatency = now.Sub(sendingTime).Seconds() - execReport.Duration - execReport.InitTime
	execReport.SchedAction = SCHED_ACTION_OFFLOAD

	return response.ExecutionReport, nil
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
