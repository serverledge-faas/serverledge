package workflow

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/serverledge-faas/serverledge/internal/client"
	"golang.org/x/exp/slices"
	"io"
	"net/http"
	"sort"
	"time"

	"github.com/serverledge-faas/serverledge/internal/cache"
	"github.com/serverledge-faas/serverledge/utils"
	"log"

	"github.com/serverledge-faas/serverledge/internal/asl"
	"github.com/serverledge-faas/serverledge/internal/function"
	"github.com/serverledge-faas/serverledge/internal/types"
)

// WorkflowInvocationResumeRequest is a request to resume the execution of a workflow (typically on a remote node)
// TODO: move in another file?
type WorkflowInvocationResumeRequest struct {
	ReqId string
	client.WorkflowInvocationRequest
	Plan ExecutionPlan
}

var offloadingPolicy OffloadingPolicy = &NoOffloadingPolicy{}

//&NoOffloadingPolicy{} // TODO: handle initialization elsewhere

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

func (workflow *Workflow) Find(taskId TaskId) (Task, bool) {
	task, found := workflow.Tasks[taskId]
	return task, found
}

// add can be used to add a new task to the Workflow. Does not chain anything, but updates Workflow width
func (workflow *Workflow) add(task Task) {
	workflow.Tasks[task.GetId()] = task // if already exists, overwrites!
}

func (w *Workflow) GetPreviousTasks(task Task) []TaskId {
	if w.prevTasks == nil {
		w.computePreviousTasks()
	}

	return w.prevTasks[task.GetId()]
}

func (w *Workflow) GetAllPreviousTasks() map[TaskId][]TaskId {
	if w.prevTasks == nil {
		w.computePreviousTasks()
	}

	return w.prevTasks
}

func (w *Workflow) computePreviousTasks() {
	w.prevTasks = make(map[TaskId][]TaskId)
	visited := make(map[TaskId]bool)
	for tid, _ := range w.Tasks {
		w.prevTasks[tid] = make([]TaskId, 0)
		visited[tid] = false
	}

	toVisit := []Task{w.Start}

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
				if !slices.Contains(w.prevTasks[nextTask], task.GetId()) {
					w.prevTasks[nextTask] = append(w.prevTasks[nextTask], task.GetId())
				}
				if !visited[nextTask] {
					toVisit = append(toVisit, w.Tasks[nextTask])
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

func (workflow *Workflow) doNothingExec(progress *Progress, input *PartialData, task UnaryTask, r *Request) (*PartialData, *Progress, bool, error) {

	output := input.Data
	outputData := NewPartialData(ReqId(r.Id), task.GetNext(), output)

	progress.Complete(task.GetId())

	shouldContinueExecution := task.GetType() != Fail && task.GetType() != Succeed
	if shouldContinueExecution {
		err := progress.AddReadyTask(task.GetNext())
		if err != nil {
			return nil, progress, false, nil
		}
	}

	return outputData, progress, shouldContinueExecution, nil
}

func (workflow *Workflow) Execute(r *Request, input *PartialData, progress *Progress) (*PartialData, *Progress, bool, error) {
	var output *PartialData
	var err error
	shouldContinue := true

	var nextTasks []TaskId

	if r.Plan == nil {
		nextTasks = progress.ReadyToExecute
	} else {
		for _, t := range progress.ReadyToExecute {
			for _, other := range r.Plan.ToExecute {
				if t == other {
					nextTasks = append(nextTasks, t)
				}
			}
		}
	}

	if len(nextTasks) >= 1 {
		n, ok := workflow.Find(nextTasks[0])
		if !ok {
			return nil, progress, true, fmt.Errorf("failed to find task %s", n.GetId())
		}

		switch task := n.(type) {
		case *FunctionTask:
			output, progress, shouldContinue, err = task.execute(progress, input, r)
		case *ChoiceTask:
			output, progress, shouldContinue, err = task.execute(progress, input, r)
		case *StartTask:
			output, progress, shouldContinue, err = task.execute(progress, input)
		case *PassTask:
			output, progress, shouldContinue, err = workflow.doNothingExec(progress, input, task, r)
		case *FailureTask:
			output, progress, shouldContinue, err = task.execute(progress, r)
		case *SuccessTask:
			output, progress, shouldContinue, err = workflow.doNothingExec(progress, input, task, r)
		case *EndTask:
			output, progress, shouldContinue, err = task.execute(progress, input)
		}
		if err != nil {
			progress.Fail(n.GetId())
			return output, progress, false, err
		}
	} else {
		err = SaveProgress(progress)
		if err != nil {
			return nil, progress, false, err
		}
		err = SavePartialData(input)
		if err != nil {
			return nil, progress, false, err
		}
		return nil, progress, false, nil
	}

	return output, progress, shouldContinue, nil
}

// GetUniqueFunctions returns a list with the function names used in the Workflow. The returned function names are unique and in alphabetical order
func (workflow *Workflow) GetUniqueFunctions() []string {
	allFunctionsMap := make(map[string]interface{})
	for _, task := range workflow.Tasks {
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

func (workflow *Workflow) getEtcdKey() string {
	return getEtcdKey(workflow.Name)
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
func (workflow *Workflow) Save() error {
	if len(workflow.Name) == 0 {
		return fmt.Errorf("cannot save an anonymous workflow (no name set)")
	}

	cli, err := utils.GetEtcdClient()
	if err != nil {
		return err
	}
	ctx := context.TODO()

	// marshal the workflow object into json
	payload, err := json.Marshal(*workflow)
	if err != nil {
		return fmt.Errorf("could not marshal workflow: %v", err)
	}
	// saves the json object into etcd
	_, err = cli.Put(ctx, workflow.getEtcdKey(), string(payload))
	if err != nil {
		return fmt.Errorf("failed etcd Put: %v", err)
	}

	// Add the workflow to the local cache
	cache.GetCacheInstance().Set(workflow.Name, workflow, cache.DefaultExp)

	return nil
}

// Invoke schedules each function of the workflow and invokes them
func (workflow *Workflow) Invoke(r *Request) error {

	var err error
	requestId := ReqId(r.Id)

	var progress *Progress
	var pd *PartialData

	if !r.Resuming {
		progress = InitProgress(requestId, workflow)
		pd = NewPartialData(requestId, workflow.Start.Id, r.Params)
	} else {
		progress, err = RetrieveProgress(requestId)
		if err != nil {
			return fmt.Errorf("failed to retrieve workflow progress: %v", err)
		}
		if len(progress.ReadyToExecute) == 0 {
			return fmt.Errorf("workflow resumed but no task is ready for execution: %v", requestId)
		} else if len(progress.ReadyToExecute) > 1 {
			// TODO: manage case when len is > 1 (e.g., parallel branches)
			return fmt.Errorf("workflow resumed with multiple tasks ready for execution not yet implemented!: %v", requestId)
		}

		pds, err := RetrievePartialData(requestId, progress.ReadyToExecute[0])
		if err != nil {
			return fmt.Errorf("workflow resumed but unable to retrieve partial data of next task: %v", progress.ReadyToExecute[0])
		}
		if len(pds) != 1 {
			return fmt.Errorf("expected 1 partial data for next task: %v", progress.ReadyToExecute[0])
		}
		pd = pds[0] // TODO: to be updated when refactoring parallel orchestration
	}

	shouldContinue := true
	for shouldContinue {
		decision, err := offloadingPolicy.Evaluate(r, progress)
		if err == nil && decision.Offload {

			err := SaveProgress(progress)
			if err != nil {
				return fmt.Errorf("Could not save progress: %v", err)
			}
			err = SavePartialData(pd)
			if err != nil {
				return fmt.Errorf("Could not save partial data: %v", err)
			}

			log.Printf("Offloading request: %v", requestId)

			shouldContinue, err = offload(r, &decision)
			if err != nil {
				return err
			}

			log.Printf("Offloading done. Should continue: %v", shouldContinue)

			if shouldContinue {
				progress, err = RetrieveProgress(requestId)
				if err != nil {
					return fmt.Errorf("Could not retrieve progress after offloading: %v", err)
				}
				pds, err := RetrievePartialData(requestId, progress.ReadyToExecute[0])
				if err != nil {
					return fmt.Errorf("Could not retrieve partial data: %v", err)
				}
				if len(pds) != 1 {
					return fmt.Errorf("expected 1 partial data for next task: %v", progress.ReadyToExecute[0])
				}
				pd = pds[0] // TODO: to be updated when refactoring parallel orchestration
				log.Printf("Ready to execute after offloading: %v", progress.ReadyToExecute)
			}
		} else {
			pd, progress, shouldContinue, err = workflow.Execute(r, pd, progress)
			if err != nil {
				return fmt.Errorf("failed workflow execution: %v", err)
			}

			if !shouldContinue && pd != nil {
				r.ExecReport.Result = pd.Data
			}
		}

	}

	// TODO: delete  progress if needed

	return nil
}

func offload(r *Request, policyDecision *OffloadingDecision) (bool, error) {

	shouldContinue := false

	log.Printf("Offloading decision: %v", policyDecision)

	request := WorkflowInvocationResumeRequest{
		ReqId: r.Id,
		WorkflowInvocationRequest: client.WorkflowInvocationRequest{
			Params:          r.Params,
			CanDoOffloading: false,
			Async:           false, // we force a synchronous request
		},
		Plan: policyDecision.ExecutionPlan,
	}
	invocationBody, err := json.Marshal(request)
	if err != nil {
		return shouldContinue, fmt.Errorf("JSON marshaling failed: %v", err)
	}

	// Send invocation request
	url := fmt.Sprintf("http://%s/workflow/resume/%s", policyDecision.RemoteHost, r.W.Name)
	resp, err := utils.PostJson(url, invocationBody)
	if err != nil {
		return shouldContinue, fmt.Errorf("HTTP request for offloading failed: %v", err)
	}

	if resp.StatusCode != http.StatusOK {
		return shouldContinue, fmt.Errorf("failed offloaded workflow: %v", err)
	}

	var response InvocationResponse
	body, _ := io.ReadAll(resp.Body)
	err = json.Unmarshal(body, &response)
	if err != nil {
		return shouldContinue, fmt.Errorf("Failed InvocationResponse unmarshaling: %v", err)
	}

	if !response.Success {
		return shouldContinue, fmt.Errorf("failed offloaded workflow: %v", err)
	}

	for k, v := range response.Reports {
		r.ExecReport.Reports[k] = v
	}

	if response.Result == nil {
		// workflow execution is not complete after offloading
		shouldContinue = true
	} else {
		r.ExecReport.Result = response.Result
	}

	return shouldContinue, nil
}

// Delete removes the Workflow from cache and from etcd, so it cannot be invoked anymore
func (workflow *Workflow) Delete() error {
	cli, err := utils.GetEtcdClient()
	if err != nil {
		return err
	}
	ctx := context.TODO()

	dresp, err := cli.Delete(ctx, workflow.getEtcdKey())
	if err != nil || dresp.Deleted != 1 {
		return fmt.Errorf("failed Delete: %v", err)
	}

	// Remove the function from the local cache
	cache.GetCacheInstance().Delete(workflow.Name)

	return nil
}

// Exists return true if the workflow exists either in etcd or in cache. If it only exists in Etcd, it saves the workflow also in caches
func (workflow *Workflow) Exists() bool {
	_, found := getFromCache(workflow.Name)
	if !found {
		// cache miss
		f, err := getFromEtcd(workflow.Name)
		if err != nil {
			if err.Error() == fmt.Sprintf("failed to retrieve value for key %s", getEtcdKey(workflow.Name)) {
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

func (workflow *Workflow) Equals(comparer types.Comparable) bool {

	workflow2 := comparer.(*Workflow)

	if workflow.Name != workflow2.Name {
		return false
	}

	for k := range workflow.Tasks {
		if !workflow.Tasks[k].Equals(workflow2.Tasks[k]) {
			return false
		}
	}
	return workflow.Start.Equals(workflow2.Start) &&
		workflow.End.Equals(workflow2.End) &&
		len(workflow.Tasks) == len(workflow2.Tasks)
}

func (workflow *Workflow) String() string {
	return fmt.Sprintf(`Workflow{
		Name: %s,
		Start: %s,
		Tasks: %s,
		End:   %s,
	}`, workflow.Name, workflow.Start.String(), workflow.Tasks, workflow.End.String())
}

// MarshalJSON is needed because Task is an interface
// This is automatically used when calling json.Marshal()
func (workflow *Workflow) MarshalJSON() ([]byte, error) {
	// Create a map to hold the JSON representation of the Workflow
	data := make(map[string]interface{})

	// Add the field to the map
	data["Name"] = workflow.Name
	data["Start"] = workflow.Start
	data["End"] = workflow.End
	tasks := make(map[TaskId]interface{})

	// Marshal the interface and store it as concrete task value in the map
	for taskId, task := range workflow.Tasks {
		tasks[taskId] = task
	}
	data["Tasks"] = tasks

	// Marshal the map to JSON
	return json.Marshal(data)
}

// UnmarshalJSON is needed because Task is an interface
// This is automatically used when calling json.Unmarshal()
func (workflow *Workflow) UnmarshalJSON(data []byte) error {
	// Create a temporary map to decode the JSON data
	var tempMap map[string]json.RawMessage
	if err := json.Unmarshal(data, &tempMap); err != nil {
		return err
	}
	// extract simple fields
	if rawStart, ok := tempMap["Start"]; ok {
		if err := json.Unmarshal(rawStart, &workflow.Start); err != nil {
			return err
		}
	} else {
		return fmt.Errorf("missing 'Start' field in JSON")
	}

	if rawEnd, ok := tempMap["End"]; ok {
		if err := json.Unmarshal(rawEnd, &workflow.End); err != nil {
			return err
		}
	} else {
		return fmt.Errorf("missing 'End' field in JSON")
	}

	if rawName, ok := tempMap["Name"]; ok {
		if err := json.Unmarshal(rawName, &workflow.Name); err != nil {
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
	workflow.Tasks = make(map[TaskId]Task)
	for taskId, value := range tempTaskMap {
		err := workflow.decodeTask(taskId, value)
		if err != nil {
			return err
		}
	}
	return nil
}

func (workflow *Workflow) decodeTask(taskId string, value json.RawMessage) error {
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
			workflow.Tasks[TaskId(taskId)] = task
			return nil
		}
	case Function:
		task := &FunctionTask{}
		err = json.Unmarshal(value, task)
		if err == nil && task.Id != "" && task.Func != "" {
			workflow.Tasks[TaskId(taskId)] = task
			return nil
		}
	case Choice:
		task := &ChoiceTask{}
		err = json.Unmarshal(value, task)
		if err == nil && task.Id != "" && len(task.AlternativeNextTasks) == len(task.Conditions) {
			workflow.Tasks[TaskId(taskId)] = task
			return nil
		}
	default:
		err = json.Unmarshal(value, task)
		if err == nil && task.GetId() != "" {
			workflow.Tasks[TaskId(taskId)] = task
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
func (workflow *Workflow) IsEmpty() bool {
	if len(workflow.Tasks) == 0 {
		return true
	}

	hasOnlyStartAndEnd := false
	if len(workflow.Tasks) == 2 {
		hasStart := 0
		hasEnd := 0
		for _, task := range workflow.Tasks {
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
