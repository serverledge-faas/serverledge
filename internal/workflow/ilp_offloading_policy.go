package workflow

import (
	"encoding/json"
	"github.com/serverledge-faas/serverledge/internal/function"
	"github.com/serverledge-faas/serverledge/internal/metrics"
	"github.com/serverledge-faas/serverledge/internal/node"
	"github.com/serverledge-faas/serverledge/internal/registration"
	"golang.org/x/exp/slices"
	"strconv"
)

type IlpOffloadingPolicy struct{}

type ilpParams struct {
	N            []string            `json:"N"`            // Set of nodes
	NodeMemory   map[string]float64  `json:"node_memory"`  // Memory per node
	DSLatency    map[string]float64  `json:"ds_latency"`   // Latency per node
	DSBandwidth  map[string]float64  `json:"ds_bandwidth"` // Bandwidth per node
	NodeLatency  map[string]float64  `json:"node_latency"` // map[json.dumps((src_node, dst_node))] = latency
	HandlingNode string              `json:"handling_node"`
	T            []string            `json:"T"`           // Set of tasks
	Adj          map[string][]string `json:"adj"`         // Task adjacency list
	TaskMemory   map[string]float64  `json:"task_memory"` // Memory per task
	Deadline     float64             `json:"deadline"`    // Global deadline
	OutputSize   map[string]float64  `json:"output_size"` // Output size per task
	InputSize    float64             `json:"input_size"`  // Input data size

	Alpha      float64             `json:"alpha"`       // Weighting factor
	NodeLabels map[string][]string `json:"node_labels"` // Labels per node
	TaskLabels map[string][]string `json:"task_labels"` // Labels per task

	ExecTime map[string]float64 `json:"exectime"` // map[json.dumps((task, node))] = time
}

const CLOUD = "CLOUD"
const LOCAL = "LOCAL"

func tupleKey(s1, s2 string) string {
	keyBytes, _ := json.Marshal([]string{s1, s2})
	return string(keyBytes)
}

func computeOutputSize(workflow *Workflow, inputParamsSize float64) map[string]float64 {

	task := workflow.Start

	tasks := make([]Task, 0)
	visited := make(map[TaskId]bool)
	toVisit := []Task{task}

	outputSize := make(map[string]float64)
	avgOutputSize := metrics.GetMetrics().AvgOutputSize

	var currentInputSize float64 // TODO: propagating input size to output only works for sequential connections

	for len(toVisit) > 0 {
		task := toVisit[0]
		tasks = append(tasks, task)
		toVisit = toVisit[1:]
		visited[task.GetId()] = true

		var currentOutputSize float64

		var nextTasks []TaskId
		switch typedTask := task.(type) {
		case ConditionalTask:
			nextTasks = typedTask.GetAlternatives()
			currentOutputSize = currentInputSize
		case UnaryTask:
			nextTasks = append(nextTasks, typedTask.GetNext())

			ft, ok := typedTask.(*FunctionTask)
			if ok {
				currentOutputSize = avgOutputSize[ft.Func]
			} else {
				currentOutputSize = currentInputSize
			}
		default:
			currentOutputSize = currentInputSize
		}

		outputSize[string(task.GetId())] = currentOutputSize

		for _, nt := range nextTasks {
			if _, ok := visited[nt]; !ok {
				nextTask, ok := workflow.Tasks[nt]
				if ok {
					if !slices.Contains(toVisit, nextTask) {
						toVisit = append(toVisit, nextTask)
					}
				}
			}
		}

		currentInputSize = currentOutputSize
	}

	return outputSize
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
		// TODO: we do not handle scheduling during workflow execution at the moment
		return OffloadingDecision{Offload: false}, nil
	}

	// TODO: prepare parameters for ILP
	params := ilpParams{}
	params.N = []string{LOCAL, CLOUD}
	params.Deadline = r.QoS.MaxRespT
	params.HandlingNode = LOCAL
	params.NodeMemory[LOCAL] = (float64)(node.Resources.AvailableMemMB)

	// Add available Edge peers
	nearbyServers := registration.Reg.NearbyServersMap
	if nearbyServers != nil {
		for k, v := range nearbyServers {
			if v.AvailableMemMB > 0 && v.AvailableCPUs > 0 {
				params.N = append(params.N, k)
				params.NodeMemory[k] = float64(v.AvailableMemMB)
			}
		}
	}

	// Compute distances
	for key1, v1 := range nearbyServers {
		var distance float64
		if !slices.Contains(params.N, key1) {
			continue
		}
		distance = registration.Reg.Client.DistanceTo(&v1.Coordinates).Seconds() / 2
		params.NodeLatency[tupleKey(LOCAL, key1)] = distance
		params.NodeLatency[tupleKey(key1, LOCAL)] = distance

		for key2, v2 := range nearbyServers {
			if !slices.Contains(params.N, key2) {
				continue
			}
			if key1 == key2 {
				distance = 0.0
			} else {
				distance = v1.Coordinates.DistanceTo(&v2.Coordinates).Seconds() / 2
			}
			params.NodeLatency[tupleKey(key1, key2)] = distance
			params.NodeLatency[tupleKey(key2, key1)] = distance
		}
	}

	// Distances to Cloud and Data Store
	// TODO: we assume distance to Cloud == distance to DS (for all Edge nodes)
	distanceToCloud := 0.100 // TODO
	for _, n := range params.N {
		if n == CLOUD {
			params.NodeLatency[tupleKey(n, n)] = 0.0
			params.DSLatency[n] = 0.0
		} else {
			params.NodeLatency[tupleKey(n, CLOUD)] = distanceToCloud
			params.NodeLatency[tupleKey(CLOUD, n)] = distanceToCloud
			params.DSLatency[n] = distanceToCloud
		}
	}

	// Bandwidth (we assume identical)
	dsBandwidth := 100.0 // TODO
	for _, n := range params.N {
		if n == CLOUD {
			params.DSBandwidth[n] = dsBandwidth * 10
		} else {
			params.DSBandwidth[n] = dsBandwidth
		}
	}

	// Workflow
	// TODO: we cannot marshal every time just to know the size!!
	data, _ := json.Marshal(r.Params)
	params.InputSize = float64(len(data))
	params.OutputSize = computeOutputSize(r.W, params.InputSize)

	params.T = make([]string, len(r.W.Tasks))
	for tid, task := range r.W.Tasks {
		params.T = append(params.T, string(tid))
		params.Adj[string(tid)] = make([]string, 1)

		switch typedTask := task.(type) {
		case ConditionalTask:
			nextTasks := typedTask.GetAlternatives()
			for _, nextTask := range nextTasks {
				probability := 1.0 / float64(len(nextTasks)) // TODO
				entry := tupleKey(string(nextTask), strconv.FormatFloat(probability, 'f', 2, 32))
				params.Adj[string(tid)] = append(params.Adj[string(tid)], entry)
			}
			params.TaskMemory[string(tid)] = float64(10)
		case UnaryTask:
			nextTid := string(typedTask.GetNext())
			entry := tupleKey(nextTid, "1.0")
			params.Adj[string(tid)] = append(params.Adj[string(tid)], entry)

			ft, ok := typedTask.(*FunctionTask)
			if ok {
				f, _ := function.GetFunction(ft.Func)
				params.TaskMemory[string(tid)] = float64(f.MemoryMB)
			}

		default:
			params.TaskMemory[string(tid)] = float64(10)
		}

	}

	// TODO: resolve ILP

	// TODO: parse results and make a decision

	//if completed >= 2 && completed < 4 {
	//	plan := ExecutionPlan{ToExecute: p.ReadyToExecute} // TODO
	//	return OffloadingDecision{true, "127.0.0.1:1323", plan}, nil
	//}

	return OffloadingDecision{Offload: false}, nil
}
