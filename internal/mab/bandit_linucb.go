package mab

import (
	"log"
	"math"
	"sync"

	"github.com/serverledge-faas/serverledge/internal/config"
	"gonum.org/v1/gonum/mat" // for matrix operations
)

// LinUCBDisjointPolicy implements the LinUCB algorithm with disjoint linear models.
// Reference: Li et al., "A Contextual-Bandit Approach to Personalized News Article Recommendation", Algorithm 1.
type LinUCBDisjointPolicy struct {
	Alpha float64 // Exploration parameter

	// Maps each arm to its features (A, b)
	Arms map[string]*LinUCBArmState
	mu   sync.RWMutex

	// Dimension of the feature vector (d)
	// Bias (1) + MemoryFeature (1) = 2
	Dim int
}

// LinUCBArmState holds the matrix A and vector b for a specific arm.
// A represents the design matrix (d x d)
// b represents the reward mapping (d x 1)
type LinUCBArmState struct {
	A *mat.Dense
	b *mat.VecDense
}

// NewLinUCBDisjointPolicy creates a new instance of the policy.
func NewLinUCBDisjointPolicy(alpha float64) *LinUCBDisjointPolicy {
	return &LinUCBDisjointPolicy{
		Alpha: alpha,
		Arms:  make(map[string]*LinUCBArmState),
		Dim:   2, // Currently: Bias + Memory Usage
	}
}

// InitArm initializes the matrices for a new architecture.
func (p *LinUCBDisjointPolicy) InitArm(arm string) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if _, exists := p.Arms[arm]; exists {
		return
	}

	// Initialize A as Identity Matrix (d x d) as per the paper
	A := mat.NewDense(p.Dim, p.Dim, nil)
	for i := 0; i < p.Dim; i++ {
		A.Set(i, i, 1.0)
	}

	// Initialize b as Zero Vector (d)
	b := mat.NewVecDense(p.Dim, nil)

	p.Arms[arm] = &LinUCBArmState{
		A: A,
		b: b,
	}
}

// SelectArm calculates the UCB score for each arm using the context and returns the best one.
func (p *LinUCBDisjointPolicy) SelectArm(ctx *Context) string {
	p.mu.Lock()
	defer p.mu.Unlock()

	bestArm := ""
	bestScore := -math.MaxFloat64

	for arm, state := range p.Arms {
		// Construct Feature Vector x_t for this arm
		// We need the memory usage specifically for THIS arm from the context
		memUsage, ok := ctx.ArchMemUsage[arm]
		if !ok {
			// If no info let's assume it is a new arm and therefore it has no functions running.
			memUsage = 0.0
		}

		x := p.computeFeatures(memUsage)

		// Compute Inverse of A
		var AInv mat.Dense
		err := AInv.Inverse(state.A)
		if err != nil {
			log.Printf("[LinUCB] Error inverting matrix for arm %s: %v", arm, err)
			panic(1) // it should never happen
		}

		// Compute Theta_hat = A_inv * b (line 8 of Algorithm 1 in the aforementioned paper
		var theta mat.VecDense
		theta.MulVec(&AInv, state.b)

		// Compute x^T * theta
		expectedReward := mat.Dot(x, &theta)

		// Compute alpha * sqrt(x^T * A_inv * x)
		var tempVec mat.VecDense
		tempVec.MulVec(&AInv, x)
		variance := mat.Dot(x, &tempVec)
		confidence := p.Alpha * math.Sqrt(variance)

		// Final UCB Score for this arm
		score := expectedReward + confidence

		log.Printf("[LinUCB] Arm: %s, Mem: %.2f, Exp: %.4f, Conf: %.4f, Score: %.4f", arm, memUsage, expectedReward, confidence, score)

		if score > bestScore {
			bestScore = score
			bestArm = arm
		}
	}

	if bestArm == "" {
		log.Println("[LinUCB] Warning: No arms available/configured. Panic.")
		panic(2) // should never happen if initialized correctly
	}

	return bestArm
}

// UpdateReward updates A and b for the chosen arm. Context is necessary to keep track of the memory usage AT THE MOMENT
// the decision was taken. So it has to be a "snapshot" of memory at that given time.
func (p *LinUCBDisjointPolicy) UpdateReward(arm string, ctx *Context, isWarmStart bool, durationMs float64) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if !isWarmStart {
		return // likely an outlier, skip update
	}

	state, ok := p.Arms[arm]
	if !ok {
		log.Printf("[LinUCB] Warning: Trying to update unknown arm %s", arm)
		panic(3) // should never happen if correctly used
	}

	// Reconstruct the feature vector x_t used at decision time
	memUsage := 0.0
	if ctx != nil {
		memUsage = ctx.ArchMemUsage[arm]
	} else {
		log.Printf("[LinUCB] Warning: Context is nil for arm %s", arm)
		panic(4) // should never happen
	}
	lambda := config.GetFloat(config.MAB_LINUCB_LAMBDA, 0.0)
	// reward as negative Log to handle better very slow and very fast exec times plus eventual memory penalty
	reward := -math.Log(durationMs) - (lambda * memPenalty(memUsage))
	x := p.computeFeatures(memUsage)

	// Update A: A = A + x * x^T
	var outerProduct mat.Dense
	outerProduct.Outer(1.0, x, x)
	state.A.Add(state.A, &outerProduct)

	// Update b: b = b + reward * x
	var scaledX mat.VecDense
	scaledX.ScaleVec(reward, x)
	state.b.AddVec(state.b, &scaledX)
}

func memPenalty(memUsage float64) float64 {
	// Grows from 0 at 0.75 utilization to 1 at 1.0 utilization
	penalty := (memUsage - 0.75) / 0.25 // (memUsage - 0.75) / (1 - 0.75)
	return max(0.0, penalty)
}

// computeFeatures transforms raw context data into the feature vector [1, sigma(u)].
func (p *LinUCBDisjointPolicy) computeFeatures(memUsage float64) *mat.VecDense {
	// Bias term
	bias := 1.0

	// Non-linear penalty (sigma) as suggested: 1 / (1 - u + epsilon)
	// epsilon prevents division by zero if usage is 100%
	epsilon := 0.01
	sigma := 1.0 / (1.0 - memUsage + epsilon)

	return mat.NewVecDense(p.Dim, []float64{bias, sigma})
}

func (p *LinUCBDisjointPolicy) GetType() BanditType {
	return LinUCB
}
