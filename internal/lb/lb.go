package lb

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"time"

	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	"github.com/serverledge-faas/serverledge/internal/config"
	"github.com/serverledge-faas/serverledge/internal/mab"
	"github.com/serverledge-faas/serverledge/internal/registration"
)

var currentTargets []*middleware.ProxyTarget

func newBalancer(targets []*middleware.ProxyTarget) middleware.ProxyBalancer {
	// old Load Balancer: return middleware.NewRoundRobinBalancer(targets)
	return NewArchitectureAwareBalancer(targets)
}

func StartReverseProxy(e *echo.Echo, region string) {
	targets, err := getTargets(region)
	if err != nil {
		log.Printf("Cannot connect to registry to retrieve targets: %v\n", err)
		os.Exit(2)
	}

	log.Printf("Initializing with %d targets.\n", len(targets))
	balancer := newBalancer(targets)
	currentTargets = targets

	// Custom ProxyConfig to process custom headers and update available memory of each targets after they
	// executed a function.
	// These headers are set after the execution of the function on the target node, so the free memory already
	// includes the memory freed by the function, once it's executed.
	proxyConfig := middleware.ProxyConfig{
		Balancer: balancer,

		// We use ModifyResponse to process these headers
		ModifyResponse: func(res *http.Response) error {

			// Here we read the body, and then we restore it. This is done to avoid a potential race condition:
			// the main thread of this LB will send the body back to the original user/caller, since it's acting as a
			// reverse proxy. In the meantime UpdateBandit will try to read the same stream of data to get the
			// stats about the execution, to update the bandit. Even if the goroutine tries to restore the response after
			// reading it, chaches are that that won't happen quick eough (and it will not be a reliable solution anyway),
			// so the solution here is the following:
			// 1. We read the response body
			// 2. We extract the fileds needed to update the bandit, and we pass those to UpdateBandit
			// 3. We restore the response body so that it can be read by the ProxyBalancer in order to send it back to the user.
			// All of this is because we don't want to call UpdateBandit synchronously, since that would add more
			// latency for the final user.
			bodyBytes, err := io.ReadAll(res.Body)
			if err != nil {
				return err
			}
			_ = res.Body.Close()

			res.Body = io.NopCloser(bytes.NewBuffer(bodyBytes)) // reset the body, as discussed earlier

			// Extract the necessary data for UpdateBandit
			nodeArch := res.Header.Get("Serverledge-Node-Arch")
			reqPath := res.Request.URL.Path
			reqID := res.Request.Header.Get("Serverledge-MAB-Request-ID")

			go func(data []byte, path string, arch string, reqID string) {
				err := mab.UpdateBandit(data, path, arch, reqID)
				if err != nil {
					log.Printf("Failed to update bandit: %v", err)
				}
			}(bodyBytes, reqPath, nodeArch, reqID)

			nodeName := res.Header.Get("Serverledge-Node-Name")
			freeMemStr := res.Header.Get("Serverledge-Free-Mem")
			freeCpuStr := res.Header.Get("Serverledge-Free-CPU")
			timestampStr := res.Header.Get("Serverledge-Timestamp")

			if nodeName != "" && freeMemStr != "" {
				freeMem, err := strconv.ParseInt(freeMemStr, 10, 64)
				freeCpu, err2 := strconv.ParseFloat(freeCpuStr, 64)
				timestamp, err3 := strconv.ParseInt(timestampStr, 10, 64)

				if err == nil && err2 == nil && err3 == nil {
					NodeMetrics.Update(nodeName, freeMem, 0, timestamp, freeCpu)

					log.Printf("[LB-Update] Node %s reported %d MB free", nodeName, freeMem)
				} else {
					log.Printf("ERROR updating node stats: MEM error: %v, CPU error: %v", err, err2)
				}
			}

			// Remove the no-longer-needed headers
			res.Header.Del("Serverledge-Node-Name")
			res.Header.Del("Serverledge-Free-Mem")
			res.Header.Del("Serverledge-Free-CPU")
			res.Header.Del("Serverledge-MAB-Request-ID")

			// for experiments: we need to know which node ran the function
			res.Header.Set("Serverledge-Node-Arch", nodeArch)

			return nil
		},
	}

	e.Use(middleware.ProxyWithConfig(proxyConfig))
	go updateTargets(balancer, region)

	portNumber := config.GetInt(config.API_PORT, 1323)
	if err := e.Start(fmt.Sprintf(":%d", portNumber)); err != nil && !errors.Is(err, http.ErrServerClosed) {
		e.Logger.Fatal("shutting down the server")
	}
}

func getTargets(region string) ([]*middleware.ProxyTarget, error) {
	cloudNodes, err := registration.GetNodesInArea(region, false, 0)
	if err != nil {
		return nil, err
	}

	targets := make([]*middleware.ProxyTarget, 0, len(cloudNodes))
	for _, target := range cloudNodes {
		log.Printf("Found target: %v\n", target.Key)
		// TODO: etcd should NOT contain URLs, but only host and port...
		parsedUrl, err := url.Parse(target.APIUrl())
		if err != nil {
			return nil, err
		}
		archMap := echo.Map{"arch": target.Arch}
		targets = append(targets, &middleware.ProxyTarget{Name: target.Key, URL: parsedUrl, Meta: archMap})
	}

	log.Printf("Found %d targets\n", len(targets))

	return targets, nil
}

func updateTargets(balancer middleware.ProxyBalancer, region string) {
	var sleepTime = config.GetInt(config.LB_REFRESH_INTERVAL, 30)
	for {
		time.Sleep(time.Duration(sleepTime) * time.Second)
		log.Printf("[LB]: Periodic targets update\n")

		targets, err := getTargets(region)
		if err != nil {
			log.Printf("Cannot update targets: %v\n", err)
			continue // otherwise we update everything with a nil target array, removing all targets from the LB list!
		}

		toKeep := make([]bool, len(currentTargets))
		for i := range currentTargets {
			toKeep[i] = false
		}
		for _, t := range targets {
			toAdd := true
			for i, curr := range currentTargets {
				if curr.Name == t.Name {
					toKeep[i] = true
					toAdd = false
					// Since we're keeping this node, we'll update it's free memory info.
					nodeInfo := GetSingleTargetInfo(curr)
					if nodeInfo != nil {
						totalMemory := nodeInfo.TotalMemory
						freeMemoryMB := totalMemory - nodeInfo.UsedMemory
						freeCpu := nodeInfo.TotalCPU - nodeInfo.UsedCPU
						NodeMetrics.Update(curr.Name, freeMemoryMB, totalMemory, nodeInfo.LastUpdateTime, freeCpu)
					}

				}
			}
			if toAdd {
				log.Printf("Adding %s\n", t.Name)
				balancer.AddTarget(t)
			}
		}

		toRemove := make([]string, 0)
		for i, curr := range currentTargets {
			if !toKeep[i] {
				log.Printf("Removing %s\n", curr.Name)
				toRemove = append(toRemove, curr.Name)
			} else {
				// If we keep this node, then we'll update its info about free memory
				nodeInfo := GetSingleTargetInfo(curr)
				if nodeInfo != nil {
					totalMemory := nodeInfo.TotalMemory
					freeMemoryMB := totalMemory - nodeInfo.UsedMemory
					freeCpu := nodeInfo.TotalCPU - nodeInfo.UsedCPU
					NodeMetrics.Update(curr.Name, freeMemoryMB, totalMemory, nodeInfo.LastUpdateTime, freeCpu)
				}
			}
		}
		for _, curr := range toRemove {
			balancer.RemoveTarget(curr)
		}

		currentTargets = targets
	}
}

func GetSingleTargetInfo(target *middleware.ProxyTarget) *registration.StatusInformation {

	// Build the status URL and GET request to the target (not using UDP best-effort implementation)
	targetUrl := fmt.Sprintf("%s/status", target.URL)

	resp, err := http.Get(targetUrl)
	if err != nil {
		log.Printf("Failed to get status from target %s: %v", target.Name, err)
		return nil
	}
	defer func(Body io.ReadCloser) {
		err := Body.Close()
		if err != nil {
			log.Printf("Failed to close response body: %v", err)
		}
	}(resp.Body)

	// Decode the JSON response to obtain the StatusInfo data structure
	var statusInfo registration.StatusInformation
	err = json.NewDecoder(resp.Body).Decode(&statusInfo)
	if err != nil {
		log.Printf("Failed to decode status response from target %s: %v", target.Name, err)
		return nil
	}

	return &statusInfo
}
