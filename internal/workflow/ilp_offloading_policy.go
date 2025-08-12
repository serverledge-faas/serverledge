package workflow

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/serverledge-faas/serverledge/internal/config"
	"github.com/serverledge-faas/serverledge/internal/function"
	"github.com/serverledge-faas/serverledge/internal/metrics"
	"github.com/serverledge-faas/serverledge/internal/node"
	"github.com/serverledge-faas/serverledge/internal/registration"
	"golang.org/x/exp/slices"
)

type IlpOffloadingPolicy struct{}

type ilpParams struct {
	CloudNodes   []string            `json:"cloud_nodes"`  // Set of Cloud nodes
	EdgeNodes    []string            `json:"edge_nodes"`   // Set of Edge nodes
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

type taskPlacement map[TaskId]string // TODO: might be useful to have a Node type with utility functions to retrieve node info...

func initParams() ilpParams {
	return ilpParams{
		Adj:         make(map[string][]string),
		ExecTime:    make(map[string]float64),
		OutputSize:  make(map[string]float64),
		NodeMemory:  make(map[string]float64),
		TaskMemory:  make(map[string]float64),
		NodeLabels:  make(map[string][]string),
		TaskLabels:  make(map[string][]string),
		DSBandwidth: make(map[string]float64),
		DSLatency:   make(map[string]float64),
		NodeLatency: make(map[string]float64),
	}
}

const CLOUD = "CLOUD"
const LOCAL = "LOCAL"

var httpClient = &http.Client{Timeout: 10 * time.Second}

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

	// Prepare parameters for ILP
	params := initParams()
	params.CloudNodes = []string{CLOUD}
	params.EdgeNodes = []string{LOCAL}
	params.Deadline = r.QoS.MaxRespT
	params.HandlingNode = LOCAL
	params.NodeMemory[LOCAL] = (float64)(node.Resources.AvailableMemMB)

	// TODO: introduce task and node labels

	// Add available Edge peers
	nearbyServers := registration.NeighborInfo
	if nearbyServers != nil {
		for k, v := range nearbyServers {
			if v.AvailableMemMB > 0 && v.AvailableCPUs > 0 {
				params.EdgeNodes = append(params.EdgeNodes, k)
				params.NodeMemory[k] = float64(v.AvailableMemMB)
			}
		}
	}

	// Compute distances
	for key1, v1 := range nearbyServers {
		var distance float64
		if !slices.Contains(params.EdgeNodes, key1) {
			continue
		}
		distance = registration.VivaldiClient.DistanceTo(&v1.Coordinates).Seconds() / 2
		params.NodeLatency[tupleKey(LOCAL, key1)] = distance
		params.NodeLatency[tupleKey(key1, LOCAL)] = distance

		for key2, v2 := range nearbyServers {
			if !slices.Contains(params.EdgeNodes, key2) {
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

	// Execution Times
	retrievedTimes := metrics.GetMetrics().AvgExecutionTimeAllNodes
	for tid, task := range r.W.Tasks {
		ft, ok := task.(*FunctionTask)
		if ok {
			f, _ := function.GetFunction(ft.Func)
			for _, n := range params.EdgeNodes {
				nodeTimes, found := retrievedTimes[n]
				if !found {
					params.ExecTime[tupleKey(string(tid), n)] = 1 // TODO: just guessing
					continue
				}
				t, found := nodeTimes[f.Name]
				if found {
					params.ExecTime[tupleKey(string(tid), n)] = t
				} else {
					params.ExecTime[tupleKey(string(tid), n)] = 1 // TODO: just guessing
				}
			}
			params.ExecTime[tupleKey(string(tid), CLOUD)] = 0.1 // TODO: how to retrieve cloud metrics? cloud nodes should use unique identifier??
		} else {
			for _, n := range params.EdgeNodes {
				params.ExecTime[tupleKey(string(tid), n)] = 0.0001
			}
			params.ExecTime[tupleKey(string(tid), CLOUD)] = 0.0001
		}
	}

	// Distances to Cloud and Data Store
	// TODO: we assume distance to Cloud == distance to DS (for all Edge nodes)
	distanceToCloud := 0.100 // TODO: measure Cloud latency (ping? or, retrieve from offloadingLatency )
	for _, n := range params.EdgeNodes {
		params.NodeLatency[tupleKey(n, CLOUD)] = distanceToCloud
		params.NodeLatency[tupleKey(CLOUD, n)] = distanceToCloud
		params.DSLatency[n] = distanceToCloud
	}
	params.NodeLatency[tupleKey(CLOUD, CLOUD)] = 0.0
	params.DSLatency[CLOUD] = 0.001

	// Bandwidth (we assume identical)
	dsBandwidth := config.GetFloat(config.OFFLOADING_POLICY_NODE_TO_DATA_STORE_BANDWIDTH, 100.0)
	for _, n := range params.EdgeNodes {
		params.DSBandwidth[n] = dsBandwidth
	}
	params.DSBandwidth[CLOUD] = config.GetFloat(config.OFFLOADING_POLICY_CLOUD_TO_DATA_STORE_BANDWIDTH, dsBandwidth*10)

	// Workflow
	params.InputSize = float64(r.ParamsSize)
	params.OutputSize = computeOutputSize(r.W, params.InputSize)

	params.T = make([]string, 0)
	for tid, task := range r.W.Tasks {
		params.T = append(params.T, string(tid))
		params.Adj[string(tid)] = make([]string, 0)

		switch typedTask := task.(type) {
		case ConditionalTask:
			nextTasks := typedTask.GetAlternatives()
			branchFrequency := metrics.GetMetrics().BranchFrequency
			// check if metrics have value
			sum := 0.0
			for _, nextTask := range nextTasks {
				sum += branchFrequency[string(tid)][string(nextTask)]
			}
			for _, nextTask := range nextTasks {
				var probability float64
				if sum == 0.0 {
					// fallback if no metrics
					probability = 1.0 / float64(len(nextTasks))
				} else {
					probability = branchFrequency[string(tid)][string(nextTask)]
				}
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
			} else {
				params.TaskMemory[string(tid)] = float64(10)
			}

		default:
			params.TaskMemory[string(tid)] = float64(10)
		}

	}

	// Serialize to JSON
	jsonData, err := json.Marshal(params)
	if err != nil {
		panic(err)
	}

	// Create POST request
	ilpOptimizerHost := config.GetString(config.OFFLOADING_POLICY_ILP_OPTIMIZER_HOST, "localhost")
	ilpOptimizerPort := config.GetInt(config.OFFLOADING_POLICY_ILP_OPTIMIZER_PORT, 8080)
	url := fmt.Sprintf("http://%s:%d", ilpOptimizerHost, ilpOptimizerPort)
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

	// Read and print response
	var placement taskPlacement
	err = json.NewDecoder(resp.Body).Decode(&placement)
	if err != nil {
		fmt.Println(err)
		return OffloadingDecision{Offload: false}, fmt.Errorf("decoding response: %w", err)
	}

	for k, v := range placement {
		fmt.Printf("Task: %s -> %s \n", k, v)
	}

	// TODO: parse results and make a decision

	//if completed >= 2 && completed < 4 {
	//	plan := OffloadingPlan{ToExecute: p.ReadyToExecute} // TODO
	//	return OffloadingDecision{true, "127.0.0.1:1323", plan}, nil
	//}

	return OffloadingDecision{Offload: false}, nil
}
