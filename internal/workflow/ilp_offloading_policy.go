package workflow

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/serverledge-faas/serverledge/internal/node"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/serverledge-faas/serverledge/internal/config"
	"github.com/serverledge-faas/serverledge/internal/function"
	"github.com/serverledge-faas/serverledge/internal/metrics"
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

	ObjWeights []float64           `json:"obj_weights"` // Objective terms weights
	Cost       map[string]float64  `json:"cost"`        // Computation cost
	NodeLabels map[string][]string `json:"node_labels"` // Labels per node
	TaskLabels map[string][]string `json:"task_labels"` // Labels per task

	ExecTime map[string]float64 `json:"exectime"`  // map[json.dumps((task, node))] = time
	InitTime map[string]float64 `json:"init_time"` // map[json.dumps((task, node))] = time
}

type taskPlacement map[TaskId]string

func initParams() ilpParams {
	return ilpParams{
		Adj:         make(map[string][]string),
		ExecTime:    make(map[string]float64),
		InitTime:    make(map[string]float64),
		OutputSize:  make(map[string]float64),
		NodeMemory:  make(map[string]float64),
		Cost:        make(map[string]float64),
		TaskMemory:  make(map[string]float64),
		NodeLabels:  make(map[string][]string),
		TaskLabels:  make(map[string][]string),
		DSBandwidth: make(map[string]float64),
		DSLatency:   make(map[string]float64),
		NodeLatency: make(map[string]float64),
		ObjWeights:  []float64{0.33, 0.33, 0.33},
	}
}

const CLOUD = "CLOUD"

var httpClient = &http.Client{Timeout: 10 * time.Second}

type cachedPlacement struct {
	placement taskPlacement
	ttl       int
}

var placementCache map[string]*cachedPlacement

func getCachedSolution(r *Request) (*taskPlacement, bool) {
	if placementCache == nil {
		placementCache = make(map[string]*cachedPlacement)
		return nil, false
	}

	sol, ok := placementCache[r.Id]
	if !ok {
		return nil, false
	}

	// check TTL
	if sol.ttl > 0 {
		sol.ttl--
		return &sol.placement, ok
	}

	delete(placementCache, r.Id)
	return nil, false
}

func cacheSolution(r *Request, sol *taskPlacement) {
	if placementCache == nil {
		placementCache = make(map[string]*cachedPlacement)
	}

	defaultTTL := config.GetInt(config.OFFLOADING_POLICY_ILP_PLACEMENT_TTL, 2) - 1

	placementCache[r.Id] = &cachedPlacement{
		placement: *sol,
		ttl:       defaultTTL,
	}
}

func tupleKey(s1, s2 string) string {
	keyBytes, _ := json.Marshal([]string{s1, s2})
	return string(keyBytes)
}

// TODO: Update this function as soon as tasks that split/merge their inputs are implemented (e.g., Map)
func computeOutputSize(workflow *Workflow, inputParamsSize float64) map[string]float64 {

	task := workflow.Start

	tasks := make([]Task, 0)
	visited := make(map[TaskId]bool)
	toVisit := []Task{task}

	outputSize := make(map[string]float64)
	avgOutputSize := metrics.GetMetrics().AvgOutputSize

	var currentInputSize = inputParamsSize

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

	var LOCAL = registration.SelfRegistration.Key

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

	// Prepare parameters for ILP
	params := initParams()
	params.CloudNodes = make([]string, 0)
	if registration.GetRemoteOffloadingTarget() != nil {
		params.CloudNodes = append(params.CloudNodes, CLOUD)
	}
	params.EdgeNodes = []string{LOCAL}
	params.Deadline = r.QoS.MaxRespT - time.Now().Sub(r.Arrival).Seconds()
	params.HandlingNode = LOCAL
	params.NodeMemory[LOCAL] = (float64)(node.Resources.AvailableMemMB)

	wViolations := config.GetFloat(config.OFFLOADING_POLICY_ILP_OBJ_WEIGHT_VIOLATIONS, 0.33)
	wDataTransfers := config.GetFloat(config.OFFLOADING_POLICY_ILP_OBJ_WEIGHT_DATA_TRANSFERS, 0.33)
	wCost := config.GetFloat(config.OFFLOADING_POLICY_ILP_OBJ_WEIGHT_COST, 0.33)
	params.ObjWeights = []float64{wViolations, wDataTransfers, wCost}

	regionCost := config.GetStringMapFloat64(config.OFFLOADING_POLICY_ILP_REGION_COST)
	// TODO: ToLower() is needed because viper (used to parse configuration files) is not case sensitive
	localCost, ok := regionCost[strings.ToLower(registration.SelfRegistration.Area)]
	if !ok {
		localCost = 0.0
	}
	params.Cost[LOCAL] = localCost

	// TODO: introduce task and node labels

	// Add available Edge peers
	nearbyServers := registration.GetFullNeighborInfo()
	if nearbyServers != nil {
		for k, v := range nearbyServers {
			if v.AvailableMemMB > 0 && v.AvailableCPUs > 0 {
				params.EdgeNodes = append(params.EdgeNodes, k)
				params.NodeMemory[k] = float64(v.AvailableMemMB)

				// Cost (assuming that Edge nodes are all in the same area)
				params.Cost[k] = localCost
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

	if len(params.CloudNodes) > 0 {
		// Cost
		// TODO: ToLower() is needed because viper (used to parse configuration files) is not case sensitive
		remoteCost, ok := regionCost[strings.ToLower(registration.GetRemoteOffloadingTarget().Area)]
		if !ok {
			remoteCost = 0.0
		}
		params.Cost[CLOUD] = remoteCost

		// Distances to Cloud and Data Store
		distanceToCloud := registration.GetRemoteOffloadingTargetLatencyMs() / 1000.0
		for _, n := range params.EdgeNodes {
			params.NodeLatency[tupleKey(n, CLOUD)] = distanceToCloud
			params.NodeLatency[tupleKey(CLOUD, n)] = distanceToCloud

			// TODO: we assume distance to Cloud == distance to DS (for all Edge nodes)
			params.DSLatency[n] = distanceToCloud
		}
		params.NodeLatency[tupleKey(CLOUD, CLOUD)] = 0.0
		params.DSLatency[CLOUD] = 0.001
	} else {
		distanceToDS := 0.100 // TODO: how to compute?
		for _, n := range params.EdgeNodes {
			params.DSLatency[n] = distanceToDS
		}
	}

	// Bandwidth (we assume identical)
	dsBandwidth := config.GetFloat(config.OFFLOADING_POLICY_NODE_TO_DATA_STORE_BANDWIDTH, 100.0)
	for _, n := range params.EdgeNodes {
		params.DSBandwidth[n] = dsBandwidth
	}

	if len(params.CloudNodes) > 0 {
		params.DSBandwidth[CLOUD] = config.GetFloat(config.OFFLOADING_POLICY_CLOUD_TO_DATA_STORE_BANDWIDTH, dsBandwidth*10)
	}

	localWarmStatus := node.WarmStatus()

	// Execution Times
	retrievedMetrics := metrics.GetMetrics()
	for tid, task := range r.W.Tasks {
		if p.Status[tid] == Executed || p.Status[tid] == Skipped {
			continue
		}
		ft, ok := task.(*FunctionTask)
		if ok {
			f, _ := function.GetFunction(ft.Func)
			for _, n := range params.EdgeNodes {
				nId := node.NodeID{Area: registration.SelfRegistration.Area, Key: n}
				// Execution Times
				nodeTimes, found := retrievedMetrics.AvgEdgeExecutionTime[nId.String()]
				if !found {
					log.Printf("No data about exec times on %s", n)
					params.ExecTime[tupleKey(string(tid), n)] = 0.01 // no data: just guessing
				} else {
					t, found := nodeTimes[f.Name]
					if found {
						params.ExecTime[tupleKey(string(tid), n)] = t
					} else {
						log.Printf("No data about exec times of %s on %s", f.Name, n)
						params.ExecTime[tupleKey(string(tid), n)] = 0.01 // no data: just guessing
					}
				}

				// Init Times
				initTimes, found := retrievedMetrics.AvgEdgeInitTime[nId.String()]
				if !found {
					// Unknown node
					params.InitTime[tupleKey(string(tid), n)] = 0.01 // no data: just guessing
					continue
				}

				coldStart := false
				if n == LOCAL {
					warmCount, ok := localWarmStatus[f.Name]
					if !ok || warmCount < 1 {
						coldStart = true
					}
				} else {
					warmCount, ok := nearbyServers[n].AvailableWarmContainers[f.Name]
					if !ok || warmCount < 1 {
						coldStart = true
					}
				}

				if !coldStart {
					params.InitTime[tupleKey(string(tid), n)] = 0
				} else {
					t, found := initTimes[f.Name]
					if found {
						params.InitTime[tupleKey(string(tid), n)] = t
					} else {
						params.InitTime[tupleKey(string(tid), n)] = 0.01 // no data: just guessing
					}
				}
			}

			if len(params.CloudNodes) > 0 {
				// Cloud Execution Time
				t, found := retrievedMetrics.AvgRemoteExecutionTime[f.Name]
				if found {
					params.ExecTime[tupleKey(string(tid), CLOUD)] = t
				} else {
					params.ExecTime[tupleKey(string(tid), CLOUD)] = 0.01 // no data: just guessing
				}

				// Init Time
				coldStart := true // TODO: assuming cold start
				if !coldStart {
					params.InitTime[tupleKey(string(tid), CLOUD)] = 0
				} else {
					t, found = retrievedMetrics.AvgRemoteInitTime[f.Name]
					if found {
						params.InitTime[tupleKey(string(tid), CLOUD)] = t
					} else {
						params.InitTime[tupleKey(string(tid), CLOUD)] = 0.01 // no data: just guessing
					}
				}
			}
		} else {
			// The task is not a Functiontask
			for _, n := range params.EdgeNodes {
				params.ExecTime[tupleKey(string(tid), n)] = 0.0001
				params.InitTime[tupleKey(string(tid), n)] = 0.0
			}
			if len(params.CloudNodes) > 0 {
				params.ExecTime[tupleKey(string(tid), CLOUD)] = 0.0001
				params.InitTime[tupleKey(string(tid), CLOUD)] = 0.0
			}
		}
	}

	// Workflow
	params.InputSize = float64(r.ParamsSize)
	params.OutputSize = computeOutputSize(r.W, params.InputSize)

	params.T = make([]string, 0)
	for tid, task := range r.W.Tasks {

		if p.Status[tid] == Skipped || p.Status[tid] == Executed {
			continue
		}
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

	cacheSolution(r, &placement)

	for k, v := range placement {
		fmt.Printf("Task: %s -> %s \n", k, v)
	}

	// parse results and make a decision
	return computeDecisionFromPlacement(placement, p, r), nil
}

func computeDecisionFromPlacement(placement taskPlacement, p *Progress, r *Request) OffloadingDecision {

	var localExecution = false
	var remoteNode string
	for _, t := range p.ReadyToExecute {
		assignedNode := placement[t]
		if assignedNode == registration.SelfRegistration.Key {
			localExecution = true
			break
		} else {
			remoteNode = assignedNode
		}
	}

	if localExecution {
		log.Println("Continuing with local execution")
		return OffloadingDecision{Offload: false}
	}

	// Retrieve all tasks assigned to n
	toExecute := make([]TaskId, 0)
	for t, assignedNode := range placement {
		if assignedNode == remoteNode {
			toExecute = append(toExecute, t)
		}
	}

	plan := OffloadingPlan{ToExecute: toExecute}

	var remoteNodeReg *registration.NodeRegistration
	if remoteNode == CLOUD {
		remoteNodeReg = registration.GetRemoteOffloadingTarget()
	} else {
		remoteNodeReg = registration.GetPeerFromKey(remoteNode)
	}

	decision := OffloadingDecision{true, remoteNodeReg.APIUrl(), plan}
	log.Printf("Decision: %v\n", decision)
	return decision
}
