package workflow

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/serverledge-faas/serverledge/internal/config"
	"log"
	"net/http"
)

type HEFTlessPolicy struct{}

func (policy *HEFTlessPolicy) Evaluate(r *Request, p *Progress) (OffloadingDecision, error) {

	completed := 0

	if p == nil || !r.CanDoOffloading || len(p.ReadyToExecute) == 0 {
		return OffloadingDecision{Offload: false}, nil
	}

	for _, s := range p.Status {
		if s == Executed {
			completed++
		}
	}

	if completed > 0 {
		placement, found := getCachedSolution(r)
		if found {
			log.Printf("Reusing cached placement\n")
			return computeDecisionFromPlacement(*placement, p, r), nil
		} else {
			// No rescheduling admitted
			return OffloadingDecision{Offload: false}, fmt.Errorf("HEFTless does not support re-scheduling during execution")
		}
	}

	params := prepareParameters(r, p)
	if r.QoS.MaxRespT <= 0.0 {
		// User did not specify a deadline
		params.Deadline = 99999
	}

	// Serialize to JSON
	jsonData, err := json.Marshal(params)
	if err != nil {
		panic(err)
	}

	// Create POST request
	optimizerHost := config.GetString(config.WORKFLOW_OFFLOADING_POLICY_OPTIMIZER_HOST, "localhost")
	optimizerPort := config.GetInt(config.WORKFLOW_OFFLOADING_POLICY_OPTIMIZER_PORT, 8080)
	url := fmt.Sprintf("http://%s:%d/heftless", optimizerHost, optimizerPort)
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonData))
	if err != nil {
		return OffloadingDecision{Offload: false}, fmt.Errorf("creating request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	// Send the request
	resp, err := httpClient.Do(req)
	if err != nil {
		fmt.Println(err)
		return OffloadingDecision{Offload: false}, fmt.Errorf("sending request: %w", err)
	}
	defer resp.Body.Close()

	statusCode := resp.StatusCode
	if statusCode != 200 {
		return OffloadingDecision{Offload: false}, fmt.Errorf("scheduling failed with status code %d", statusCode)
	}

	// Read and print response
	var placement taskPlacement
	err = json.NewDecoder(resp.Body).Decode(&placement)
	if err != nil {
		fmt.Println(err)
		return OffloadingDecision{Offload: false}, fmt.Errorf("decoding response: %w", err)
	}

	cacheSolution(r, &placement, 9999)

	for k, v := range placement {
		fmt.Printf("Task: %s -> %s \n", k, v)
	}

	// parse results and make a decision
	return computeDecisionFromPlacement(placement, p, r), nil
}
