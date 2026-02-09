package workflow

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/serverledge-faas/serverledge/internal/config"
	"log"
	"net/http"
	"time"
)

type IlpOffloadingPolicy struct{}

func (policy *IlpOffloadingPolicy) Init() {
}

func (policy *IlpOffloadingPolicy) Evaluate(r *Request, p *Progress) (OffloadingDecision, error) {

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
		}
	}

	params := prepareParameters(r, p)

	t0 := time.Now()

	// Serialize to JSON
	jsonData, err := json.Marshal(params)
	if err != nil {
		panic(err)
	}

	// Create POST request
	ilpOptimizerHost := config.GetString(config.WORKFLOW_OFFLOADING_POLICY_OPTIMIZER_HOST, "localhost")
	ilpOptimizerPort := config.GetInt(config.WORKFLOW_OFFLOADING_POLICY_OPTIMIZER_PORT, 8080)
	url := fmt.Sprintf("http://%s:%d/ilp", ilpOptimizerHost, ilpOptimizerPort)
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

	solverTime := time.Since(t0)
	log.Printf("solver time for %s: %v\n", r.W.Name, solverTime.Seconds())

	// Read and print response
	var placement taskPlacement
	err = json.NewDecoder(resp.Body).Decode(&placement)
	if err != nil {
		fmt.Println(err)
		return OffloadingDecision{Offload: false}, fmt.Errorf("decoding response: %w", err)
	}

	defaultTTL := config.GetInt(config.WORKFLOW_OFFLOADING_POLICY_ILP_PLACEMENT_TTL, 2) - 1
	cacheSolution(r, &placement, defaultTTL)

	for k, v := range placement {
		fmt.Printf("Task: %s -> %s \n", k, v)
	}

	// parse results and make a decision
	return computeDecisionFromPlacement(placement, p, r), nil
}
