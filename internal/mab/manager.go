package mab

import (
	"log"
	"sync"

	"github.com/serverledge-faas/serverledge/internal/config"
)

// BanditManager contains all the existing bandits (one for each known function)
type BanditManager struct {
	bandits map[string]Policy // Nota: ora è map[string]Policy, non *UCB1Bandit
	mu      sync.RWMutex
}

var GlobalBanditManager *BanditManager

// InitBanditManager sets up the bandit manager
func InitBanditManager() {
	GlobalBanditManager = &BanditManager{
		bandits: make(map[string]Policy),
	}
}

// GetBandit returns (or creates) the bandit for a given function
func (bm *BanditManager) GetBandit(functionName string) Policy {
	bm.mu.Lock()
	defer bm.mu.Unlock()

	if _, exists := bm.bandits[functionName]; !exists {
		// Read policy from config
		policyType := config.GetString(config.MAB_POLICY, "UCB1")
		log.Printf("BanditManager GetBandit: policy type: %s\n", policyType)

		var newBandit Policy

		switch policyType {
		case "LinUCB":
			// Alpha param could also be in config
			alpha := config.GetFloat(config.MAB_LINUCB_ALPHA, 0.1)
			newBandit = NewLinUCBDisjointPolicy(alpha)
			log.Printf("Initialized LinUCB bandit for %s", functionName)
		default:
			// Default to UCB1 (Legacy)
			newBandit = &UCB1Bandit{
				TotalCounts: 0,
				Arms:        map[string]*ArmStats{},
				c:           config.GetFloat(config.MAB_UCB1_C, 0.8),
			}
			log.Printf("Initialized UCB1 bandit for %s", functionName)
		}

		// Ideally, this list is not hardcoded, but comes from the LB or Discovery or the config
		newBandit.InitArm("amd64")
		newBandit.InitArm("arm64")

		bm.bandits[functionName] = newBandit
	}
	return bm.bandits[functionName]
}
