package mab

import (
	"encoding/json"
	"fmt"
	"log"
	"strings"

	"github.com/serverledge-faas/serverledge/internal/function"
)

func UpdateBandit(body []byte, reqPath string, arch string, reqID string) error { // Read the body
	// Parse the body to a Response object
	var response function.Response
	if err := json.Unmarshal(body, &response); err != nil {
		return fmt.Errorf("failed to unmarshal response body: %v", err)
	}
	// get the url of the request, to extract the function name, so that we can update the related MAB.
	pathParts := strings.Split(reqPath, "/")
	if len(pathParts) < 3 || pathParts[len(pathParts)-2] != "invoke" {
		return fmt.Errorf("could not extract function name from URL: %s", reqPath)
	}
	functionName := pathParts[len(pathParts)-1]

	bandit := GlobalBanditManager.GetBandit(functionName)
	ctx := GlobalContextStorage.RetrieveAndDelete(reqID)

	if arch == "" {
		log.Println("Serverledge-Node-Arch header missing")
		panic(0) // should never happen
	}

	// Calculate the reward for this execution
	if response.ExecutionReport.Duration <= 0 {
		log.Printf("invalid execution duration: %f", response.ExecutionReport.Duration)
		panic(1) // should never happen
	}

	// Reward = 1 / Duration (we don't consider cold start delay, since we want to focus on architectures' performance)
	durationMs := response.ExecutionReport.Duration * 1000.0 // s to ms

	// finally update the reward for the bandit. This is thread safe since internally it has a mutex
	bandit.UpdateReward(arch, ctx, response.IsWarmStart, durationMs)

	return nil
}
