package mab

import (
	"log"
	"math"
	"sync"
)

// NOTE: Since nomenclature may be confusing: 'ARM' is the architecture, 'arm' is the arm of the Multi-Armed Bandit (MAB)

// ArmStats maintains information about a single arm dedicated to a single function
type ArmStats struct {
	Count      int64   // UCB needs to know hom many times we chose that arm/architecture
	SumRewards float64 // Sum of rewards
	AvgReward  float64 // Avg Reward (Q value in the formula)
}

// UCB1Bandit is the bandit that handles decision for ONE function
type UCB1Bandit struct {
	TotalCounts int64                // number of total executions (t)
	Arms        map[string]*ArmStats // Map "amd64" -> Stats, "arm64" -> Stats for each arm
	mu          sync.RWMutex         // Mutex per thread-safety
	c           float64              // Exploration parameter C (usually sqrt(2) ~= 1.41, but can be tuned)
	// Higher values lead to more exploration. Lower values lead to more exploitation.
}

// InitArm adds a new arm to the bandit
func (b *UCB1Bandit) InitArm(arm string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, exists := b.Arms[arm]; !exists {
		b.Arms[arm] = &ArmStats{Count: 0, SumRewards: 0, AvgReward: 0}
	}
}

// SelectArm implements UCB-1 formulas
// Returns the suggested architecture to use ("amd64" o "arm64").
// ctx *Ctx is necessary even if not used to be compliant with the interface.
func (b *UCB1Bandit) SelectArm(ctx *Context) string {
	b.mu.Lock()
	defer b.mu.Unlock()

	ctx = nil // not used, favor garbage collection
	minSampleCount := int64(1)
	currentMinSample := int64(math.MaxInt64)
	leastTriedArch := ""

	// 1. If an arm hasn't tried at least minSampleCount times, it has to be tried. If both haven't reached this threshold,
	// we choose the one with fewer tries.
	for arch, stats := range b.Arms {
		if stats.Count < minSampleCount && stats.Count < currentMinSample {
			currentMinSample = stats.Count
			leastTriedArch = arch
		}
	}
	if leastTriedArch != "" {
		log.Printf("Using (forced) least tried arch: %s", leastTriedArch)
		b.TotalCounts++
		b.Arms[leastTriedArch].Count = b.Arms[leastTriedArch].Count + 1
		return leastTriedArch
	}

	bestScore := -math.MaxFloat64 // Initialize with a very low score
	bestArch := ""

	// 2. Calculate UCB1 score for each architecture
	for arch, stats := range b.Arms {
		// Formula: Q(a) + c * sqrt( ln(t) / N(a) ) where Q(a) is AvgReward, t is TotalCounts, N(a) is stats.Count
		explorationBonus := b.c * math.Sqrt(math.Log(float64(b.TotalCounts))/float64(stats.Count))
		score := stats.AvgReward + explorationBonus
		log.Printf("Score for %s: %f\n", arch, score)

		if score > bestScore {
			bestScore = score // Update best score
			bestArch = arch
		}
	}
	if bestArch == "" {
		log.Printf("Couldn't select any ARM. Panic\n")
		panic(1)
	}
	b.TotalCounts++
	b.Arms[bestArch].Count = b.Arms[bestArch].Count + 1

	return bestArch
}

// UpdateReward updates bandit stats after execution. For now reward is 1.0 / executionTime (not considering setup time).
// It may be fine-tuned in the future. ctx *Context is need even if it's unused to be compliant with the interface.
func (b *UCB1Bandit) UpdateReward(arch string, ctx *Context, isWarmStart bool, durationMs float64) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if ctx != nil {
		ctx = nil // is not used here but will still be set in MAB mode, help garbage collection to get rid of this
	}
	if !isWarmStart { // redact this run if it was not a warm start. Likely to be an outlier.
		b.TotalCounts--
		b.Arms[arch].Count--
		return
	}

	if _, ok := b.Arms[arch]; !ok {
		return // Should not happen
	}

	stats := b.Arms[arch]

	// reward calculation
	reward := -math.Log(durationMs) // reward as negative Log to handle better very slow and very fast exec times

	// Update average reward
	stats.SumRewards += reward
	stats.AvgReward = stats.SumRewards / float64(stats.Count)
}

func (b *UCB1Bandit) GetType() BanditType {
	return UCB1
}
