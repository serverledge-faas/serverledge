package workflow

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"maps"
	"net/http"
	"sort"
	"time"

	"github.com/serverledge-faas/serverledge/internal/node"

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

var offloadingPolicy OffloadingPolicy = nil

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
		case FanOutTask:
			nextTasks = append(nextTasks, typedTask.GetBranches()...)
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
		case FanOutTask:
			nextTasks = append(nextTasks, typedTask.GetBranches()...)
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

func (wflow *Workflow) ExecuteTask(r *Request, taskToExecute TaskId, input *TaskData, progress *Progress) (*TaskData, []TaskId, error) {
	var outputData *TaskData
	var nextTasks []TaskId

	n, ok := wflow.Find(taskToExecute)
	if !ok {
		return nil, nextTasks, fmt.Errorf("failed to find task %s", n.GetId())
	}

	// Check if the current task defines any pre-processing operations.
	// If present, execute them immediately to mutate the input data before the task execution begins.
	if ops := n.GetPreProcessors(); len(ops) > 0 {
		err := ApplyPreProcessors(input, ops)
		if err != nil {
			r.mu.Lock()
			progress.Fail(n.GetId())
			r.mu.Unlock()
			return nil, nextTasks, fmt.Errorf("failed to pre-process data for task %s: %w", n.GetId(), err)
		}
	}

	switch task := n.(type) {
	case UnaryTask:
		output, err := task.execute(input, r)

		r.mu.Lock()
		defer r.mu.Unlock()

		if err != nil {
			progress.Fail(n.GetId())
			return nil, nextTasks, err
		}

		outputData = NewTaskData(output)
		//progress.Complete(task.GetId())

		nextTask := task.GetNext()
		nextTasks = append(nextTasks, nextTask)

	case ConditionalTask:
		nextTaskId, err := task.Evaluate(input, r)

		r.mu.Lock()
		defer r.mu.Unlock()

		if err != nil {
			progress.Fail(n.GetId())
			return nil, nextTasks, err
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
		//progress.Complete(task.GetId())

		outputData = NewTaskData(input.Data)
		nextTasks = append(nextTasks, nextTaskId)

		// Update metrics, if enabled
		if metrics.Enabled {
			metrics.AddBranchCount(string(task.GetId()), string(nextTaskId))
		}
	case FanOutTask:
		outputData = NewTaskData(input.Data)

		for _, nextTask := range task.GetBranches() {
			nextTasks = append(nextTasks, nextTask)
		}

	case *EndTask:
		r.mu.Lock()
		defer r.mu.Unlock()
		//progress.Complete(task.GetId())
		outputData = input
	}

	return outputData, nextTasks, nil
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

// initializeOrRetrieveProgress initializes the progress object if it is a new workflow request or retrieves it from a resuming workflow request.
func (wflow *Workflow) initializeOrRetrieveProgress(r *Request) *Progress {
	var progress *Progress
	requestId := ReqId(r.Id)

	if !r.Resuming {
		progress = InitProgress(requestId, wflow)
		return progress
	} else {
		progress = &r.Progress
		return progress
	}
}

func (wflow *Workflow) savePartialDataForReadyTasks(r *Request, requestId ReqId, progress *Progress, data map[TaskId]*TaskData) error {
	handledTasks := make(map[TaskId]bool)
	outputData := make(map[TaskId]TaskData)

	tasksToSave := append(progress.ReadyToExecute, r.NextTasksNotEligible...)

	for _, task := range tasksToSave {
		for _, prev := range wflow.GetPreviousTasks(task) {
			if _, found := handledTasks[prev]; found {
				continue
			}

			dataToSave, ok := data[prev]
			if ok {
				outputData[prev] = *dataToSave
			} else {
				// PD not available locally
			}

			handledTasks[prev] = true
		}
	}

	r.OutputData = outputData
	return nil
}

type taskResult interface {
	TaskID() TaskId
	Error() error
}

type localExecutionResult struct {
	tid       TaskId
	out       *TaskData
	err       error
	nextTasks []TaskId
}

func (l localExecutionResult) TaskID() TaskId { return l.tid }
func (l localExecutionResult) Error() error   { return l.err }

type offloadExecutionResult struct {
	jobId                TaskId
	err                  error
	resultingProgress    Progress
	executedPlan         []TaskId
	nextTasksNotEligible []TaskId
	outputData           map[TaskId]TaskData
}

func (o offloadExecutionResult) TaskID() TaskId { return o.jobId }
func (o offloadExecutionResult) Error() error   { return o.err }

// Invoke schedules each function of the workflow and invokes them
func (wflow *Workflow) Invoke(r *Request) error {
	//alwaysSaveProgress := config.GetBool(config.WORKFLOW_ALWAYS_SAVE_PROGRESS, false)

	requestId := ReqId(r.Id)
	var err error

	progress := wflow.initializeOrRetrieveProgress(r)

	// Initialize map of TaskData
	dataMap := make(map[TaskId]*TaskData)

	// Retrieve input data from the request for tasks ready to execute (resuming workflow)
	if r.InitialData != nil {
		for task, data := range r.InitialData {
			dataMap[task] = &data
		}
		log.Printf("[Rq-%v] Injected %d pre-loaded task data from resume request", requestId, len(r.InitialData))
	}

	if len(progress.ReadyToExecute) == 0 {
		return fmt.Errorf("[Rq-%v] wflow resumed but no task is ready for execution", requestId)
	}

	log.Printf("[Rq-%v] Starting/resuming execution (%d to executed)", requestId, len(progress.ReadyToExecute))

	localReadyChan := make(chan TaskId, len(wflow.Tasks))
	resultChan := make(chan taskResult, len(wflow.Tasks))
	dispatchSignalChan := make(chan struct{}, 1)

	runningTasks := make(map[TaskId]bool)

	var finalResultData *TaskData

	// remoteNotEligible contains tasks evaluated as not eligible by a remote node.
	var remoteNotEligible []TaskId

	// Non-blocking trigger: signals the dispatcher or drops if a signal is already pending
	triggerDispatch := func() {
		select {
		case dispatchSignalChan <- struct{}{}:
		default:
		}
	}

	// Evaluates policies and dispatches ready tasks
	dispatchReadyTasks := func() error {
		r.mu.Lock()
		defer r.mu.Unlock()

		if len(progress.ReadyToExecute) == 0 {
			return nil
		}

		t0 := time.Now()
		decisions, err := offloadingPolicy.Evaluate(r, progress, runningTasks)
		policyTime := time.Since(t0).Seconds()
		r.ExecReport.SchedulingTime += policyTime

		if err != nil {
			return fmt.Errorf("an error occurred in policy evaluation: %v", err)
		}

		for _, decision := range decisions {

			if decision.Offload {

				var TasksToOffload []TaskId

				// Filter the tasks in the offloading plan to include only those that are currently ready to execute
				for _, plannedTask := range decision.OffloadingPlan.ToExecute {
					idx := slices.Index(progress.ReadyToExecute, plannedTask)
					if idx != -1 {
						TasksToOffload = append(TasksToOffload, plannedTask)

						// Remove the offloaded task directly from the slice
						progress.ReadyToExecute = append(progress.ReadyToExecute[:idx], progress.ReadyToExecute[idx+1:]...)
					}
				}

				// Skip offloading if there are no tasks actually ready
				if len(TasksToOffload) == 0 {
					continue
				}

				statusCopy := maps.Clone(progress.Status)

				remoteProgress := Progress{
					ReqId:          ReqId(r.Id),
					Status:         statusCopy,
					ReadyToExecute: TasksToOffload,
				}

				offloadJobId := TaskId(fmt.Sprintf("OFFLOAD_JOB_%v", TasksToOffload[0]))
				runningTasks[offloadJobId] = true

				dataToOffload := wflow.prepareOffloadData(decision, remoteProgress, dataMap)

				go func(jobId TaskId, dec OffloadingDecision) {
					resultingProgress, nextTasksNotEligible, outputData, errOffload := offload(r, &dec, remoteProgress, dataToOffload)

					resultChan <- offloadExecutionResult{jobId: jobId, err: errOffload, resultingProgress: resultingProgress, executedPlan: dec.ToExecute, nextTasksNotEligible: nextTasksNotEligible, outputData: outputData}
				}(offloadJobId, decision)

				log.Printf("[Rq-%v] Offloading task dispatched remotely", requestId)
			}
		}

		var tasksToKeep []TaskId

		// pick the next executable task
		for _, task := range progress.ReadyToExecute {
			// Task must be executed locally
			if r.Plan == nil || slices.Contains(r.Plan.ToExecute, task) {
				localReadyChan <- task
				continue
			}

			//Task is addressed to the source node (required when the workflow runs on a remote node)
			tasksToKeep = append(tasksToKeep, task)
		}

		progress.ReadyToExecute = tasksToKeep

		return nil
	}

	triggerDispatch()

	for {
		r.mu.Lock()
		pendingTasks := len(progress.ReadyToExecute)
		r.mu.Unlock()

		if len(runningTasks) == 0 && len(localReadyChan) == 0 && len(dispatchSignalChan) == 0 && len(remoteNotEligible) == 0 {
			if pendingTasks > 0 || (r.Resuming && len(r.NextTasksNotEligible) > 0) {
				log.Printf("[Rq-%v] Workflow has not completed but there is nothing left to execute in the plan", requestId)
				break
			}

			if finalResultData != nil {
				r.ExecReport.Result = finalResultData.Data
			}

			log.Printf("[Rq-%v] Workflow completed", requestId)

			err = DeleteAllTaskData(requestId)
			if err != nil {
				log.Printf("Failed to delete task data: %v", err)
			}

			return nil
		}

		select {
		case <-dispatchSignalChan:

			if err = dispatchReadyTasks(); err != nil {
				return err
			}

		case taskToExecute := <-localReadyChan:

			r.mu.Lock()

			log.Printf("[Rq-%v] Now going to execute %s", requestId, taskToExecute)

			var input *TaskData

			input, err = wflow.prepareInput(taskToExecute, progress, dataMap, r)
			if err != nil {
				r.mu.Unlock()
				return err
			}

			runningTasks[taskToExecute] = true

			r.mu.Unlock()

			if input == nil {
				log.Printf("Nil input for task: %s", taskToExecute)
			}

			var actualInput *TaskData
			if input != nil {
				actualInput = NewTaskData(maps.Clone(input.Data))
			}

			go func(t TaskId, in *TaskData) {
				out, nextTasks, execErr := wflow.ExecuteTask(r, t, in, progress)

				resultChan <- localExecutionResult{tid: t, out: out, err: execErr, nextTasks: nextTasks}
			}(taskToExecute, actualInput)

		case res := <-resultChan:

			r.mu.Lock()
			delete(runningTasks, res.TaskID())
			r.mu.Unlock()

			if res.Error() != nil {
				if errors.Is(res.Error(), node.OutOfResourcesErr) {
					log.Printf("[Rq-%v] Could not execute %s: out of resources", requestId, res.TaskID())
					return res.Error()
				} else {
					return fmt.Errorf("failed wflow execution: %v", res.Error())
				}
			}

			var shouldDispatch bool

			r.mu.Lock()
			switch result := res.(type) {

			case offloadExecutionResult:

				log.Printf("[Rq-%v] Remote offloading completed id: %v", requestId, result.jobId)

				remoteProgress := result.resultingProgress
				nextNotEligible := result.nextTasksNotEligible

				// Merge the data retrieved from the remote node into the local map
				for taskId, taskData := range result.outputData {
					tData := taskData
					dataMap[taskId] = &tData
				}

				finalResultData = nil

				//Update status only for tasks executed in the offload request
				for _, task := range result.executedPlan {
					if progress.Status[task] == Pending {
						progress.Status[task] = remoteProgress.Status[task]

						if _, isChoice := wflow.Tasks[task].(*ChoiceTask); isChoice && progress.Status[task] == Executed {
							for remoteTask, remoteStatus := range remoteProgress.Status {
								if remoteStatus == Skipped && progress.Status[remoteTask] == Pending {
									progress.Status[remoteTask] = Skipped
								}
							}
						}
					}
				}

				// Merge the ready tasks returned by the remote node into the local queue
				for _, remoteTask := range remoteProgress.ReadyToExecute {
					if !slices.Contains(progress.ReadyToExecute, remoteTask) {
						progress.ReadyToExecute = append(progress.ReadyToExecute, remoteTask)
					}
				}

				for _, remote := range nextNotEligible {
					// Check if the task is now eligible for execution
					if wflow.IsTaskEligibleForExecution(remote, progress) {
						for i, task := range remoteNotEligible {
							if task == remote {
								// Remove the task from the list of not eligible tasks
								remoteNotEligible = append(remoteNotEligible[:i], remoteNotEligible[i+1:]...)
							}
						}
						// Append the task to the list of ready tasks
						progress.ReadyToExecute = append(progress.ReadyToExecute, remote)
					} else {
						if !slices.Contains(remoteNotEligible, remote) {
							// Append the task to the list of not eligible tasks if it is not already there
							remoteNotEligible = append(remoteNotEligible, remote)
						}
					}
				}

				// Check if any previously not eligible tasks were completed during the offload request
				for i, notEligible := range remoteNotEligible {
					if progress.Status[notEligible] != Pending {
						remoteNotEligible = append(remoteNotEligible[:i], remoteNotEligible[i+1:]...)
					}
				}

				if r.ExecReport.Result != nil {
					log.Printf("[Rq-%v] Workflow has completed on remote node", requestId)
				}

				shouldDispatch = len(progress.ReadyToExecute) > 0
				r.mu.Unlock()

			case localExecutionResult:

				log.Printf("[Rq-%v] Executed locally: %s", requestId, result.tid)

				progress.Complete(result.tid)

				// Check if any previously not eligible tasks were completed during the local execution
				for i, nid := range remoteNotEligible {
					if nid == result.tid {
						remoteNotEligible = append(remoteNotEligible[:i], remoteNotEligible[i+1:]...)
						break
					}
				}

				// If running as an offloaded worker,
				// ensure the completed task is removed from the non-eligible list before returning the report to the coordinator.
				if r.Resuming {
					for i, nid := range r.NextTasksNotEligible {
						if nid == result.tid {
							r.NextTasksNotEligible = append(r.NextTasksNotEligible[:i], r.NextTasksNotEligible[i+1:]...)
							break
						}
					}
				}

				// tasksToExecute contains all next tasks of the just executed task (both eligible and not eligible)
				for _, nextTask := range result.nextTasks {
					if nextTask == "" {
						continue
					}
					if r.Resuming && !wflow.IsTaskEligibleForExecution(nextTask, progress) {
						if !slices.Contains(r.NextTasksNotEligible, nextTask) {
							r.NextTasksNotEligible = append(r.NextTasksNotEligible, nextTask)
						}
					} else {
						if wflow.IsTaskEligibleForExecution(nextTask, progress) {
							progress.ReadyToExecute = append(progress.ReadyToExecute, nextTask)
						}
					}
				}

				if result.out != nil {
					// Save the output in the local map
					dataMap[result.tid] = result.out

					finalResultData = result.out
				}

				shouldDispatch = len(progress.ReadyToExecute) > 0
				r.mu.Unlock()
			}

			// After processing a result, new tasks may have been added to the readyToExecute queue.
			// The dispatcher is then triggered again.
			if shouldDispatch {
				triggerDispatch()
			}
		}
	}

	// Save partial data for tasks assigned to remote nodes, ensuring distributed execution can proceed.
	if len(progress.ReadyToExecute) > 0 || len(r.NextTasksNotEligible) > 0 {
		err = wflow.savePartialDataForReadyTasks(r, requestId, progress, dataMap)
		if err != nil {
			return fmt.Errorf("Could not save partial data: %v", err)
		}
	}

	return nil
}

func offload(r *Request, policyDecision *OffloadingDecision, progress Progress, data map[TaskId]TaskData) (Progress, []TaskId, map[TaskId]TaskData, error) {

	log.Printf("[Rq-%v] Offloading decision: %v", r.Id, progress.ReadyToExecute)

	request := WorkflowInvocationResumeRequest{
		ReqId: r.Id,
		WorkflowInvocationRequest: client.WorkflowInvocationRequest{
			Params:          r.Params,
			CanDoOffloading: false,
			Async:           false, // we force a synchronous request
			QoS:             r.QoS,
		},
		Plan:     policyDecision.OffloadingPlan,
		Progress: progress,
		Data:     data,
	}

	// Update slack for deadline satisfaction
	request.QoS.MaxRespT -= time.Now().Sub(r.Arrival).Seconds()

	invocationBody, err := json.Marshal(request)
	if err != nil {
		return Progress{}, nil, nil, fmt.Errorf("JSON marshaling failed: %v", err)
	}

	// Send invocation request
	url := fmt.Sprintf("%s/workflow/resume/%s", policyDecision.RemoteHost, r.W.Name)
	resp, err := utils.PostJson(url, invocationBody)
	if err != nil {
		if resp != nil && resp.StatusCode == http.StatusTooManyRequests {
			return Progress{}, nil, nil, node.OutOfResourcesErr
		} else {
			return Progress{}, nil, nil, fmt.Errorf("HTTP request for offloading failed: %v", err)
		}
	}

	if resp.StatusCode != http.StatusOK {
		return Progress{}, nil, nil, fmt.Errorf("failed offloaded workflow: %v", err)
	}

	var response InvocationResponse
	body, _ := io.ReadAll(resp.Body)
	err = json.Unmarshal(body, &response)
	if err != nil {
		return Progress{}, nil, nil, fmt.Errorf("Failed InvocationResponse unmarshaling: %v", err)
	}

	if !response.Success {
		return Progress{}, nil, nil, fmt.Errorf("failed offloaded workflow: %v", err)
	}

	r.mu.Lock()

	for k, v := range response.Reports {
		r.ExecReport.Reports[k] = v
	}

	r.ExecReport.SchedulingTime += response.SchedulingTime

	if response.Result == nil {
		// workflow execution is not complete after offloading
		r.ExecReport.Result = nil
	} else {
		r.ExecReport.Result = response.Result
	}

	r.mu.Unlock()

	return response.ResumeData.ResultingProgress, response.ResumeData.NextTasksNotEligible, response.ResumeData.OutputData, nil
}

// prepareOffloadData collects the input data needed for the tasks to be offloaded
// by retrieving the outputs of their predecessor tasks.
func (wflow *Workflow) prepareOffloadData(plan OffloadingDecision, progress Progress, data map[TaskId]*TaskData) map[TaskId]TaskData {
	dataToOffload := make(map[TaskId]TaskData)
	handledTasks := make(map[TaskId]bool)

	for _, task := range plan.ToExecute {
		for _, prev := range wflow.GetPreviousTasks(task) {
			if _, found := handledTasks[prev]; found {
				continue
			}

			if progress.Status[prev] == Executed {
				dataToSave, ok := data[prev]
				if ok {
					dataToOffload[prev] = *dataToSave
				} else {
					// PD not available locally;
				}
			}

			handledTasks[prev] = true
		}
	}

	return dataToOffload
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
	case Parallel:
		task := &ParallelTask{}
		err = json.Unmarshal(value, task)
		if err == nil && task.Id != "" && len(task.Branches) > 0 {
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

// prepareInput resolves and prepares the input TaskData for a given task before its execution.
func (wflow *Workflow) prepareInput(taskToExecute TaskId, progress *Progress, dataMap map[TaskId]*TaskData, r *Request) (*TaskData, error) {
	task, ok := wflow.Tasks[taskToExecute]
	if !ok || task == nil {
		return nil, fmt.Errorf("task '%s' does not exist", taskToExecute)
	}

	if task.GetType() == Start {
		return NewTaskData(r.Params), nil
	}

	previousTasks := wflow.GetPreviousTasks(taskToExecute)
	var prevTasks []TaskId
	for _, previousTask := range previousTasks {
		if progress.Status[previousTask] != Skipped {
			prevTasks = append(prevTasks, previousTask)
		}
	}

	// Case 1: Multiple predecessors (logical Fan-In)
	if len(prevTasks) > 1 {
		mergedResult := make(map[string]interface{})
		parallelResults := make([]interface{}, len(prevTasks))

		for i, prevTask := range prevTasks {

			in, found := dataMap[prevTask]
			if !found {
				return nil, fmt.Errorf("partial data not found for task %s", prevTask)
			} else {
				log.Printf("[Rq-%v] Data found locally for task %v", r.Id, prevTask)
			}
			parallelResults[i] = in.Data
		}

		mergedResult["parallel_results"] = parallelResults

		// If the next task is a FunctionTask, it applies a positional mapping where the output of the i-th parallel branch
		// is mapped directly to the i-th input parameter of the function. For any other task type, the output remains unchanged.
		currentTask, ok := wflow.Find(taskToExecute)
		if !ok {
			return nil, fmt.Errorf("failed to find next task %s", taskToExecute)
		}

		fTask, isFunc := currentTask.(*FunctionTask)
		if !isFunc {
			return NewTaskData(mergedResult), nil
		}

		funct, exists := function.GetFunction(fTask.Func)
		if !exists || funct.Signature == nil {
			return NewTaskData(mergedResult), nil
		}

		delete(mergedResult, "parallel_results")

		// Positional mapping
		/*expectedInputs := funct.Signature.GetInputs()
		for i, res := range parallelResults {
			if i < len(expectedInputs) {
				targetParamName := expectedInputs[i].Name
				if resMap, ok := res.(map[string]interface{}); ok {
					for _, val := range resMap {
						mergedResult[targetParamName] = val
						break
					}
				}
			}
		}*/

		expectedInputs := funct.Signature.GetInputs()

		// Creazione di una mappa degli input attesi per il matching nominale
		expectedMap := make(map[string]bool)
		for _, input := range expectedInputs {
			expectedMap[input.Name] = true
		}

		// Estrazione e assegnazione basata sul nome della chiave, ignorando la posizione
		for _, res := range parallelResults {
			if resMap, ok := res.(map[string]interface{}); ok {
				for key, val := range resMap {
					if expectedMap[key] {
						mergedResult[key] = val
					}
				}
			}
		}

		return NewTaskData(mergedResult), nil
	}

	// Case 2: single predecessor
	if len(prevTasks) == 1 {
		previousTask := prevTasks[0]
		input, found := dataMap[previousTask]
		if !found {
			return nil, fmt.Errorf("partial data not found for task %s", previousTask)
		} else {
			log.Printf("[Rq-%v] Data found locally for task %v", r.Id, previousTask)
		}
		return input, nil
	}

	return nil, fmt.Errorf("no valid predecessor for %s", taskToExecute)
}
