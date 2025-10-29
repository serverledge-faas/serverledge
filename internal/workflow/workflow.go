package workflow

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sort"
	"time"

	"log"

	"github.com/serverledge-faas/serverledge/internal/client"
	"github.com/serverledge-faas/serverledge/internal/config"
	"github.com/serverledge-faas/serverledge/internal/metrics"
	"golang.org/x/exp/slices"

	"github.com/serverledge-faas/serverledge/internal/cache"
	"github.com/serverledge-faas/serverledge/utils"

	"github.com/serverledge-faas/serverledge/internal/asl"
	"github.com/serverledge-faas/serverledge/internal/function"
	"github.com/serverledge-faas/serverledge/internal/types"
)

var offloadingPolicy OffloadingPolicy = &IlpOffloadingPolicy{}

func CreateOffloadingPolicy() {
	policyConf := config.GetString(config.WORKFLOW_OFFLOADING_POLICY, "disable")
	log.Printf("Configured offloading policy: %s\n", policyConf)
	if policyConf == "ilp" {
		offloadingPolicy = &IlpOffloadingPolicy{}
	} else if policyConf == "heftless" {
		offloadingPolicy = &HEFTlessPolicy{}
	} else if policyConf == "threshold" {
		offloadingPolicy = &ThresholdBasedPolicy{}
	} else { // default, disable offloading
		offloadingPolicy = &NoOffloadingPolicy{}
	}

	offloadingPolicy.Init()
}

// Workflow is a Workflow to drive the execution of the workflow
type Workflow struct {
	Name  string // identifier of the Workflow
	Start *StartTask
	Tasks map[TaskId]Task
	End   *EndTask

	prevTasks map[TaskId][]TaskId
}

func newWorkflow() Workflow {
	start := NewStartTask()
	end := NewEndTask()
	tasks := make(map[TaskId]Task)
	tasks[start.Id] = start
	tasks[end.Id] = end

	workflow := Workflow{
		Start: start,
		End:   end,
		Tasks: tasks,
	}
	return workflow
}

func (wflow *Workflow) Find(taskId TaskId) (Task, bool) {
	task, found := wflow.Tasks[taskId]
	return task, found
}

// add can be used to add a new task to the Workflow. Does not chain anything, but updates Workflow width
func (wflow *Workflow) add(task Task) {
	wflow.Tasks[task.GetId()] = task // if already exists, overwrites!
}

func (wflow *Workflow) GetPreviousTasks(task TaskId) []TaskId {
	if wflow.prevTasks == nil {
		wflow.computePreviousTasks()
	}

	return wflow.prevTasks[task]
}

func (wflow *Workflow) GetAllPreviousTasks() map[TaskId][]TaskId {
	if wflow.prevTasks == nil {
		wflow.computePreviousTasks()
	}

	return wflow.prevTasks
}

func (wflow *Workflow) computePreviousTasks() {
	wflow.prevTasks = make(map[TaskId][]TaskId)
	visited := make(map[TaskId]bool)
	for tid, _ := range wflow.Tasks {
		wflow.prevTasks[tid] = make([]TaskId, 0)
		visited[tid] = false
	}

	toVisit := []Task{wflow.Start}

	for len(toVisit) > 0 {
		task := toVisit[0]
		toVisit = toVisit[1:]
		visited[task.GetId()] = true

		// task -> nextTask
		var nextTasks []TaskId
		switch typedTask := task.(type) {
		case ConditionalTask:
			nextTasks = typedTask.GetAlternatives()
		case UnaryTask:
			nextTasks = append(nextTasks, typedTask.GetNext())
		case *EndTask:
			continue
		default:
			panic("unknown task type")
		}

		for _, nextTask := range nextTasks {
			if nextTask != "" {
				if !slices.Contains(wflow.prevTasks[nextTask], task.GetId()) {
					wflow.prevTasks[nextTask] = append(wflow.prevTasks[nextTask], task.GetId())
				}
				if !visited[nextTask] {
					toVisit = append(toVisit, wflow.Tasks[nextTask])
				}
			}
		}
	}
}

func Visit(workflow *Workflow, taskId TaskId, excludeEnd bool) []Task {

	task, ok := workflow.Find(taskId)
	if !ok {
		return []Task{}
	}

	tasks := make([]Task, 0)
	visited := make(map[TaskId]bool)
	toVisit := []Task{task}

	for len(toVisit) > 0 {
		task := toVisit[0]
		tasks = append(tasks, task)
		toVisit = toVisit[1:]
		visited[task.GetId()] = true

		var nextTasks []TaskId
		switch typedTask := task.(type) {
		case ConditionalTask:
			nextTasks = typedTask.GetAlternatives()
		case UnaryTask:
			nextTasks = append(nextTasks, typedTask.GetNext())
		case *EndTask:
			continue
		default:
			panic("unknown task type: " + task.GetType())
		}

		for _, nt := range nextTasks {
			if _, ok := visited[nt]; !ok {
				nextTask, ok := workflow.Tasks[nt]
				if ok && (!excludeEnd || nextTask.GetType() != End) {
					if !slices.Contains(toVisit, nextTask) {
						toVisit = append(toVisit, nextTask)
					}
				}
			}
		}
	}

	return tasks
}

func (wflow *Workflow) IsTaskEligibleForExecution(id TaskId, p *Progress) bool {
	for _, prev := range wflow.prevTasks[id] {
		if p.Status[prev] == Pending {
			return false
		}
	}

	return true
}

func (wflow *Workflow) ExecuteTask(r *Request, taskToExecute TaskId, input *TaskData, progress *Progress) (*TaskData, error) {
	var err error
	var outputData *TaskData

	n, ok := wflow.Find(taskToExecute)
	if !ok {
		return nil, fmt.Errorf("failed to find task %s", n.GetId())
	}

	switch task := n.(type) {
	case UnaryTask:
		output, err := task.execute(input, r)
		if err != nil {
			progress.Fail(n.GetId())
			return nil, err
		}

		outputData = NewTaskData(output)
		progress.Complete(task.GetId())

		nextTask := task.GetNext()
		if wflow.IsTaskEligibleForExecution(nextTask, progress) {
			progress.ReadyToExecute = append(progress.ReadyToExecute, nextTask)
		} else {
			fmt.Printf("task %s complete, but %s not eligible for execution", task.GetId(), nextTask)
		}

	case ConditionalTask:
		nextTaskId, err := task.Evaluate(input, r)
		if err != nil {
			progress.Fail(n.GetId())
			return nil, err
		}

		// we skip all tasks that will not be executed
		toSkip := make([]Task, 0)
		toNotSkip := Visit(wflow, nextTaskId, false)
		for _, a := range task.GetAlternatives() {
			if a == nextTaskId {
				continue
			}
			branchTasks := Visit(wflow, a, false)
			for _, otherTask := range branchTasks {
				if !slices.Contains(toNotSkip, otherTask) {
					toSkip = append(toSkip, otherTask)
				}
			}
		}
		for _, t := range toSkip {
			progress.Skip(t.GetId())
		}
		progress.Complete(task.GetId())

		outputData = NewTaskData(input.Data)
		if wflow.IsTaskEligibleForExecution(nextTaskId, progress) {
			progress.ReadyToExecute = append(progress.ReadyToExecute, nextTaskId)
		}

		// Update metrics, if enabled
		if metrics.Enabled {
			metrics.AddBranchCount(string(task.GetId()), string(nextTaskId))
		}
	case *EndTask:
		progress.Complete(task.GetId())
		outputData = input
	}
	if err != nil {
		progress.Fail(n.GetId())
		return nil, err
	}

	return outputData, nil
}

// GetUniqueFunctions returns a list with the function names used in the Workflow. The returned function names are unique and in alphabetical order
func (wflow *Workflow) GetUniqueFunctions() []string {
	allFunctionsMap := make(map[string]interface{})
	for _, task := range wflow.Tasks {
		switch n := task.(type) {
		case *FunctionTask:
			allFunctionsMap[n.Func] = nil
		default:
			continue
		}
	}
	uniqueFunctions := make([]string, 0, len(allFunctionsMap))
	for fName := range allFunctionsMap {
		uniqueFunctions = append(uniqueFunctions, fName)
	}
	// we sort the list to always get the same result
	sort.Strings(uniqueFunctions)

	return uniqueFunctions
}

func (wflow *Workflow) getEtcdKey() string {
	return getEtcdKey(wflow.Name)
}

func getEtcdKey(workflowName string) string {
	return fmt.Sprintf("/workflow/%s", workflowName)
}

// GetAllWorkflows returns the workflow names
func GetAllWorkflows() ([]string, error) {
	return function.GetAllWithPrefix("/workflow")
}

func getFromCache(name string) (*Workflow, bool) {
	localCache := cache.GetCacheInstance()
	cachedObj, found := localCache.Get(name)
	if !found {
		return nil, false
	}
	//cache hit
	//return a safe copy of the workflow previously obtained
	fc := *cachedObj.(*Workflow)
	return &fc, true
}

func getFromEtcd(name string) (*Workflow, error) {
	cli, err := utils.GetEtcdClient()
	if err != nil {
		return nil, errors.New("failed to connect to ETCD")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	key := getEtcdKey(name)
	getResponse, err := cli.Get(ctx, key)
	if err != nil || len(getResponse.Kvs) < 1 {
		return nil, fmt.Errorf("failed to retrieve value for key %s", key)
	}

	var f Workflow
	err = json.Unmarshal(getResponse.Kvs[0].Value, &f)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal json: %v", err)
	}

	return &f, nil
}

// Get gets the Workflow from cache or from ETCD
func Get(name string) (*Workflow, bool) {
	val, found := getFromCache(name)
	if !found {
		// cache miss
		f, err := getFromEtcd(name)
		if err != nil {
			return nil, false
		}
		//insert a new element to the cache
		cache.GetCacheInstance().Set(name, f, cache.DefaultExp)
		return f, true
	}

	return val, true
}

// Save creates and register the workflow in Serverledge
// It is like Save for a simple function
func (wflow *Workflow) Save() error {
	if len(wflow.Name) == 0 {
		return fmt.Errorf("cannot save an anonymous wflow (no name set)")
	}

	cli, err := utils.GetEtcdClient()
	if err != nil {
		return err
	}
	ctx := context.TODO()

	// marshal the wflow object into json
	payload, err := json.Marshal(*wflow)
	if err != nil {
		return fmt.Errorf("could not marshal wflow: %v", err)
	}
	// saves the json object into etcd
	_, err = cli.Put(ctx, wflow.getEtcdKey(), string(payload))
	if err != nil {
		return fmt.Errorf("failed etcd Put: %v", err)
	}

	// Add the wflow to the local cache
	cache.GetCacheInstance().Set(wflow.Name, wflow, cache.DefaultExp)

	return nil
}

func (wflow *Workflow) initializeOrRetrieveProgress(r *Request) (*Progress, bool, error) {
	var progress *Progress
	var err error
	requestId := ReqId(r.Id)

	if !r.Resuming {
		progress = InitProgress(requestId, wflow)
		return progress, false, nil
	} else {
		progress, err = RetrieveProgress(requestId)
		if err != nil {
			return nil, true, fmt.Errorf("failed to retrieve wflow progress: %v", err)
		}
		return progress, true, nil
	}
}

func (wflow *Workflow) savePartialDataForReadyTasks(requestId ReqId, progress *Progress, data map[TaskId]*TaskData) error {
	handledTasks := make(map[TaskId]bool)

	for _, task := range progress.ReadyToExecute {
		for _, prev := range wflow.GetPreviousTasks(task) {
			if _, found := handledTasks[prev]; found {
				continue
			}

			dataToSave, ok := data[prev]
			if ok {
				err := dataToSave.Save(requestId, prev)
				if err != nil {
					return fmt.Errorf("Could not save partial data: %v", err)
				}
			} else {
				log.Printf("PD not available locally for %s; they might be on Etcd already...", prev)
			}

			handledTasks[prev] = true
		}
	}

	return nil
}

// Invoke schedules each function of the workflow and invokes them
func (wflow *Workflow) Invoke(r *Request) error {

	var err error
	requestId := ReqId(r.Id)

	progress, isProgressOnEtcd, err := wflow.initializeOrRetrieveProgress(r)
	if err != nil {
		return err
	}

	// Initialize map of TaskData
	dataMap := make(map[TaskId]*TaskData)

	if len(progress.ReadyToExecute) == 0 {
		return fmt.Errorf("wflow resumed but no task is ready for execution: %v", requestId)
	}

	for len(progress.ReadyToExecute) > 0 {
		decision, err := offloadingPolicy.Evaluate(r, progress)

		if err != nil {
			return fmt.Errorf("an error occurred in policy evaluation: %v", err)
		}

		if decision.Offload {
			err := progress.Save()
			if err != nil {
				return fmt.Errorf("Could not save progress: %v", err)
			}
			isProgressOnEtcd = true

			err = wflow.savePartialDataForReadyTasks(requestId, progress, dataMap)
			if err != nil {
				return fmt.Errorf("Could not save partial data: %v", err)
			}

			log.Printf("Offloading request: %v", requestId)

			err = offload(r, &decision)
			if err != nil {
				return err
			}

			if r.ExecReport.Result != nil {
				// Workflow execution has completed on remote node
				break
			}

			progress, err = RetrieveProgress(requestId)
			if err != nil {
				return fmt.Errorf("Could not retrieve progress after offloading: %v", err)
			}
			log.Printf("Ready to execute after offloading: %v", progress.ReadyToExecute)
		} else {
			// pick next executable task
			var taskToExecute TaskId = ""
			for _, task := range progress.ReadyToExecute {
				if r.Plan == nil || slices.Contains(r.Plan.ToExecute, task) {
					taskToExecute = task
				}
			}
			if taskToExecute == "" {
				break
			}

			// Prepare input for taskToExecute
			var input *TaskData
			if wflow.Tasks[taskToExecute].GetType() == Start {
				input = NewTaskData(r.Params)
			} else {
				var found bool
				previousTasks := wflow.GetPreviousTasks(taskToExecute)
				for _, previousTask := range previousTasks {
					if progress.Status[previousTask] == Skipped {
						continue
					}

					if input != nil {
						return fmt.Errorf("Merge of inputs not supported yet!")
					}

					input, found = dataMap[previousTask]
					if !found {
						log.Printf("Input not found in dataMap for previousTask %s", previousTask)
						input, err = RetrievePartialData(requestId, previousTask)
						if err != nil {
							return fmt.Errorf("Could not retrieve partial data: %v", err)
						}
						log.Printf("Input retrieved from etcd: %s", input)
					}
				}
			}

			if input == nil {
				log.Printf("Nil input for task: %s", taskToExecute)
			}
			output, err := wflow.ExecuteTask(r, taskToExecute, input, progress)
			if err != nil {
				return fmt.Errorf("failed wflow execution: %v", err)
			}

			dataMap[taskToExecute] = output

			if len(progress.ReadyToExecute) == 0 && output != nil {
				r.ExecReport.Result = output.Data

				if isProgressOnEtcd {
					err = DeleteProgress(requestId)
					if err != nil {
						log.Printf("Failed to delete progress: %v", err)
					}
					err = DeleteAllTaskData(requestId)
					if err != nil {
						log.Printf("Failed to delete task data: %v", err)
					}
				}

				return nil
			}
		}

	}

	if len(progress.ReadyToExecute) > 0 {
		err = progress.Save()
		if err != nil {
			return err
		}
		err = wflow.savePartialDataForReadyTasks(requestId, progress, dataMap)
		if err != nil {
			return fmt.Errorf("Could not save partial data: %v", err)
		}
	}

	return nil
}

func offload(r *Request, policyDecision *OffloadingDecision) error {

	log.Printf("Offloading decision: %v", policyDecision)

	request := WorkflowInvocationResumeRequest{
		ReqId: r.Id,
		WorkflowInvocationRequest: client.WorkflowInvocationRequest{
			Params:          r.Params,
			CanDoOffloading: false,
			Async:           false, // we force a synchronous request
			QoS:             r.QoS,
		},
		Plan: policyDecision.OffloadingPlan,
	}

	// Update slack for deadline satisfaction
	request.QoS.MaxRespT -= time.Now().Sub(r.Arrival).Seconds()

	invocationBody, err := json.Marshal(request)
	if err != nil {
		return fmt.Errorf("JSON marshaling failed: %v", err)
	}

	// Send invocation request
	url := fmt.Sprintf("%s/workflow/resume/%s", policyDecision.RemoteHost, r.W.Name)
	resp, err := utils.PostJson(url, invocationBody)
	if err != nil {
		return fmt.Errorf("HTTP request for offloading failed: %v", err)
	}

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("failed offloaded workflow: %v", err)
	}

	var response InvocationResponse
	body, _ := io.ReadAll(resp.Body)
	err = json.Unmarshal(body, &response)
	if err != nil {
		return fmt.Errorf("Failed InvocationResponse unmarshaling: %v", err)
	}

	if !response.Success {
		return fmt.Errorf("failed offloaded workflow: %v", err)
	}

	for k, v := range response.Reports {
		r.ExecReport.Reports[k] = v
	}

	if response.Result == nil {
		// workflow execution is not complete after offloading
		r.ExecReport.Result = nil
	} else {
		r.ExecReport.Result = response.Result
	}

	return nil
}

// Delete removes the Workflow from cache and from etcd, so it cannot be invoked anymore
func (wflow *Workflow) Delete() error {
	cli, err := utils.GetEtcdClient()
	if err != nil {
		return err
	}
	ctx := context.TODO()

	dresp, err := cli.Delete(ctx, wflow.getEtcdKey())
	if err != nil || dresp.Deleted != 1 {
		return fmt.Errorf("failed Delete: %v", err)
	}

	// Remove the function from the local cache
	cache.GetCacheInstance().Delete(wflow.Name)

	return nil
}

// Exists return true if the workflow exists either in etcd or in cache. If it only exists in Etcd, it saves the workflow also in caches
func (wflow *Workflow) Exists() bool {
	_, found := getFromCache(wflow.Name)
	if !found {
		// cache miss
		f, err := getFromEtcd(wflow.Name)
		if err != nil {
			if err.Error() == fmt.Sprintf("failed to retrieve value for key %s", getEtcdKey(wflow.Name)) {
				return false
			} else {
				log.Printf("ERROR: %v", err.Error())
				return false
			}
		}
		//insert a new element to the cache
		cache.GetCacheInstance().Set(f.Name, f, cache.DefaultExp)
		return true
	}
	return found
}

func (wflow *Workflow) Equals(comparer types.Comparable) bool {

	workflow2 := comparer.(*Workflow)

	if wflow.Name != workflow2.Name {
		return false
	}

	for k := range wflow.Tasks {
		if !wflow.Tasks[k].Equals(workflow2.Tasks[k]) {
			return false
		}
	}
	return wflow.Start.Equals(workflow2.Start) &&
		wflow.End.Equals(workflow2.End) &&
		len(wflow.Tasks) == len(workflow2.Tasks)
}

func (wflow *Workflow) String() string {
	return fmt.Sprintf(`Workflow{
		Name: %s,
		Start: %s,
		Tasks: %s,
		End:   %s,
	}`, wflow.Name, wflow.Start.String(), wflow.Tasks, wflow.End.String())
}

// MarshalJSON is needed because Task is an interface
// This is automatically used when calling json.Marshal()
func (wflow *Workflow) MarshalJSON() ([]byte, error) {
	// Create a map to hold the JSON representation of the Workflow
	data := make(map[string]interface{})

	// Add the field to the map
	data["Name"] = wflow.Name
	data["Start"] = wflow.Start
	data["End"] = wflow.End
	tasks := make(map[TaskId]interface{})

	// Marshal the interface and store it as concrete task value in the map
	for taskId, task := range wflow.Tasks {
		tasks[taskId] = task
	}
	data["Tasks"] = tasks

	// Marshal the map to JSON
	return json.Marshal(data)
}

// UnmarshalJSON is needed because Task is an interface
// This is automatically used when calling json.Unmarshal()
func (wflow *Workflow) UnmarshalJSON(data []byte) error {
	// Create a temporary map to decode the JSON data
	var tempMap map[string]json.RawMessage
	if err := json.Unmarshal(data, &tempMap); err != nil {
		return err
	}
	// extract simple fields
	if rawStart, ok := tempMap["Start"]; ok {
		if err := json.Unmarshal(rawStart, &wflow.Start); err != nil {
			return err
		}
	} else {
		return fmt.Errorf("missing 'Start' field in JSON")
	}

	if rawEnd, ok := tempMap["End"]; ok {
		if err := json.Unmarshal(rawEnd, &wflow.End); err != nil {
			return err
		}
	} else {
		return fmt.Errorf("missing 'End' field in JSON")
	}

	if rawName, ok := tempMap["Name"]; ok {
		if err := json.Unmarshal(rawName, &wflow.Name); err != nil {
			return err
		}
	} else {
		return fmt.Errorf("missing 'Name' field in JSON")
	}

	// Cycle on each map entry and decode the type
	var tempTaskMap map[string]json.RawMessage
	if err := json.Unmarshal(tempMap["Tasks"], &tempTaskMap); err != nil {
		return err
	}
	wflow.Tasks = make(map[TaskId]Task)
	for taskId, value := range tempTaskMap {
		err := wflow.decodeTask(taskId, value)
		if err != nil {
			return err
		}
	}
	return nil
}

func (wflow *Workflow) decodeTask(taskId string, value json.RawMessage) error {
	var tempTaskMap map[string]interface{}
	if err := json.Unmarshal(value, &tempTaskMap); err != nil {
		return err
	}
	taskType, ok := tempTaskMap["Type"].(string)
	if !ok {
		return fmt.Errorf("unknown taskType: %v", tempTaskMap["Type"])
	}
	var err error

	task := TaskFromType(TaskType(taskType))

	switch TaskType(taskType) {
	case Start:
		task := &StartTask{}
		err = json.Unmarshal(value, task)
		if err == nil && task.Id != "" {
			wflow.Tasks[TaskId(taskId)] = task
			return nil
		}
	case Function:
		task := &FunctionTask{}
		err = json.Unmarshal(value, task)
		if err == nil && task.Id != "" && task.Func != "" {
			wflow.Tasks[TaskId(taskId)] = task
			return nil
		}
	case Choice:
		task := &ChoiceTask{}
		err = json.Unmarshal(value, task)
		if err == nil && task.Id != "" && len(task.AlternativeNextTasks) == len(task.Conditions) {
			wflow.Tasks[TaskId(taskId)] = task
			return nil
		}
	default:
		err = json.Unmarshal(value, task)
		if err == nil && task.GetId() != "" {
			wflow.Tasks[TaskId(taskId)] = task
			return nil
		}
	}
	var unmarshalTypeError *json.UnmarshalTypeError
	if err != nil && !errors.As(err, &unmarshalTypeError) {
		// abort if we have an error other than the wrong type
		return err
	}

	return fmt.Errorf("failed to decode task")
}

// IsEmpty returns true if the workflow has 0 tasks or exactly one StartTask and one EndTask.
func (wflow *Workflow) IsEmpty() bool {
	if len(wflow.Tasks) == 0 {
		return true
	}

	hasOnlyStartAndEnd := false
	if len(wflow.Tasks) == 2 {
		hasStart := 0
		hasEnd := 0
		for _, task := range wflow.Tasks {
			if task.GetType() == Start {
				hasStart++
			}
			if task.GetType() == End {
				hasEnd++
			}
		}
		hasOnlyStartAndEnd = (hasStart == 1) && (hasEnd == 1)
	}

	if hasOnlyStartAndEnd {
		return true
	}

	return false
}

// findNextOrTerminate returns the State, its name and if it is terminal or not
func findNextOrTerminate(state asl.CanEnd, sm *asl.StateMachine) (asl.State, string, bool) {
	isTerminal := state.IsEndState()
	var nextState asl.State = nil
	var nextStateName = ""

	if !isTerminal {
		nextName, ok := state.(asl.HasNext).GetNext()
		if !ok {
			return nil, "", true
		}
		nextStateName = nextName
		nextState = sm.States[nextStateName]
	}
	return nextState, nextStateName, isTerminal
}
