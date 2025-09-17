package workflow

import (
	"github.com/serverledge-faas/serverledge/internal/config"
	"github.com/serverledge-faas/serverledge/internal/function"
	"github.com/serverledge-faas/serverledge/internal/node"
	"github.com/serverledge-faas/serverledge/internal/registration"
	"log"
)

type ThresholdBasedPolicy struct{}

var nodeTotalMemory float64 = -1
var utilizationThreshold float64
var maxOffloadedTasks int

func (policy *ThresholdBasedPolicy) Init() {
	nodeTotalMemory = float64(config.GetInt(config.POOL_MEMORY_MB, 1024))
	utilizationThreshold = config.GetFloat(config.WORKFLOW_THRESHOLD_BASED_POLICY_THRESHOLD, 0.75)
	maxOffloadedTasks = config.GetInt(config.WORKFLOW_THRESHOLD_BASED_POLICY_MAX_OFFLOADED, 5)
}

func (policy *ThresholdBasedPolicy) Evaluate(r *Request, p *Progress) (OffloadingDecision, error) {

	if p == nil || !r.CanDoOffloading || len(p.ReadyToExecute) == 0 {
		return OffloadingDecision{Offload: false}, nil
	}

	usedMemory := node.Resources.UsedMemMB
	nextTaskId := p.ReadyToExecute[0] // TODO: update in case of parallel branches
	nextTask := r.W.Tasks[nextTaskId]

	funcTask, ok := nextTask.(*FunctionTask)
	if !ok {
		log.Printf("Executing locally non-function task '%s'", nextTaskId)
		// not a FunctionTask
		return OffloadingDecision{Offload: false}, nil
	}

	f, found := function.GetFunction(funcTask.Func)
	if !found {
		log.Printf("Could not find function for task %s", nextTaskId)
		return OffloadingDecision{Offload: false}, nil
	}

	if float64(usedMemory+f.MemoryMB)/nodeTotalMemory <= utilizationThreshold {
		log.Printf("Threshold OK...executing locally %v", nextTaskId)
		// execute locally next task
		return OffloadingDecision{Offload: false}, nil
	}

	log.Printf("Threshold violated...must offload %v", nextTaskId)

	// Must offload
	offloadedTasks := make([]TaskId, 1)
	offloadedTasks[0] = nextTaskId
	offloadedMemory := f.MemoryMB
	nextTasks := make([]TaskId, 1)
	nextTasks[0] = funcTask.NextTask

	for len(offloadedTasks) <= maxOffloadedTasks && len(nextTasks) > 0 {
		// pop one candidate
		nextTaskId = nextTasks[0]
		nextTasks = nextTasks[1:]

		nextTask = r.W.Tasks[nextTaskId]
		switch typedTask := nextTask.(type) {
		case *FunctionTask:

			f, found = function.GetFunction(typedTask.Func)
			if !found {
				log.Printf("Could not find function for task %s", nextTaskId)
				break
			}
			if float64(usedMemory+f.MemoryMB)/nodeTotalMemory > utilizationThreshold {
				log.Printf("%v also violates threshold", nextTaskId)
				offloadedMemory += f.MemoryMB
				offloadedTasks = append(offloadedTasks, nextTaskId)

				// add successors to candidates
				nextTasks = append(nextTasks, typedTask.NextTask)
			} else {
				log.Printf("%v does not violate threshold and will be executed locally", nextTaskId)
			}
		case ConditionalTask:
			log.Printf("%v being added to offloaded group (ConditionalTask)", nextTaskId)
			offloadedTasks = append(offloadedTasks, nextTaskId)
			for _, tid := range typedTask.GetAlternatives() {
				nextTasks = append(nextTasks, tid)
			}
		default:
			// execute locally
		}

	}

	// Search for a node that can accept offloading
	nearbyServers := registration.GetFullNeighborInfo()
	offloadingTarget := ""
	offloadingTargetMem := int64(0)

	log.Printf("Total memory for offloading: %d", offloadedMemory)

	if nearbyServers != nil {
		for k, v := range nearbyServers {
			// TODO: apply a threshold here ?
			if v.AvailableMemMB >= offloadedMemory { // TODO: should look at free memory (ignoring warm containers)
				if offloadingTarget == "" || v.AvailableMemMB > offloadingTargetMem {
					offloadingTarget = k
					offloadingTargetMem = v.AvailableMemMB
				}
			} else {
				log.Printf("Not enough memory to offload to %v", k)
			}
		}
	}

	var targetNode *registration.NodeRegistration = nil

	if offloadingTarget == "" {
		// Cloud
		targetNode = registration.GetRemoteOffloadingTarget()
	} else {
		targetNode = registration.GetPeerFromKey(offloadingTarget)
	}

	if targetNode == nil {
		log.Printf("No target available for offloading")
		return OffloadingDecision{Offload: false}, nil
	}

	log.Printf("Offloading %v to %v", offloadedTasks, targetNode)
	return OffloadingDecision{Offload: true, RemoteHost: targetNode.APIUrl(), OffloadingPlan: OffloadingPlan{ToExecute: offloadedTasks}}, nil
}
