package workflow

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"

	"github.com/grussorusso/serverledge/internal/cache"
	"github.com/grussorusso/serverledge/utils"
	"github.com/labstack/gommon/log"

	"github.com/grussorusso/serverledge/internal/asl"
	"github.com/grussorusso/serverledge/internal/function"
	"github.com/grussorusso/serverledge/internal/types"
)

// used to send output from parallel nodes to fan in node or to the next node
// var outputChannel = make(chan map[string]interface{})

// Workflow is a Workflow to drive the execution of the workflow
type Workflow struct {
	Name  string     // identifier of the Workflow
	Start *StartNode // a single start must be added
	Nodes map[TaskId]Task
	End   *EndNode // a single endNode must be added
	Width int      // width is the max fanOut degree of the Workflow
}

func NewWorkflow(name string) Workflow {
	workflow := newWorkflow()
	workflow.Name = name
	return workflow
}

func newWorkflow() Workflow {
	start := NewStartNode()
	end := NewEndNode()
	nodes := make(map[TaskId]Task)
	nodes[start.Id] = start
	nodes[end.Id] = end

	workflow := Workflow{
		Start: start,
		End:   end,
		Nodes: nodes,
		Width: 1,
	}
	return workflow
}

func (workflow *Workflow) Find(nodeId TaskId) (Task, bool) {
	task, found := workflow.Nodes[nodeId]
	return task, found
}

// addNode can be used to add a new node to the Workflow. Does not chain anything, but updates Workflow width
func (workflow *Workflow) addNode(node Task) {
	workflow.Nodes[node.GetId()] = node // if already exists, overwrites!
	// updates width
	nodeWidth := node.Width()
	if nodeWidth > workflow.Width {
		workflow.Width = nodeWidth
	}
}

func isTaskPresent(node Task, infos []Task) bool {
	isPresent := false
	for _, nodeInfo := range infos {
		if nodeInfo == node {
			isPresent = true
			break
		}
	}
	return isPresent
}

func isEndNode(node Task) bool {
	_, ok := node.(*EndNode)
	return ok
}

// Visit visits the workflow starting from the input node and return a list of visited nodes. If excludeEnd = true, the EndNode will not be in the output list
func Visit(workflow *Workflow, nodeId TaskId, nodes []Task, excludeEnd bool) []Task {
	node, ok := workflow.Find(nodeId)
	if !ok {
		return []Task{}
	}
	if !isTaskPresent(node, nodes) {
		nodes = append(nodes, node)
	}
	switch n := node.(type) {
	case *StartNode:
		toAdd := Visit(workflow, n.GetNext()[0], nodes, excludeEnd)
		for _, add := range toAdd {
			if !isTaskPresent(add, nodes) {
				// only when isEndNode = true, excludeEnd = true -> we don't add the node
				if !isEndNode(add) || !excludeEnd {
					nodes = append(nodes, add)
				}
			}
		}
		return nodes
	case *SimpleNode, *PassNode, *SucceedNode, *FailNode:
		toAdd := Visit(workflow, n.GetNext()[0], nodes, excludeEnd)
		for _, add := range toAdd {
			if !isTaskPresent(add, nodes) {
				if !isEndNode(add) || !excludeEnd {
					nodes = append(nodes, add)
				}
			}
		}
		return nodes
	case *EndNode:
		if !excludeEnd { // move end node to the end of the visit list
			endNode := n
			// get index of end node to remove\
			indexToRemove := -1
			for i, task := range nodes {
				if isEndNode(task) {
					indexToRemove = i
					break
				}
			}
			// remove end node
			nodes = append(nodes[:indexToRemove], nodes[indexToRemove+1:]...)
			// append at the end of the visited node list
			nodes = append(nodes, endNode)
		}
		return nodes
	case *ChoiceNode:
		for _, alternative := range n.Alternatives {
			toAdd := Visit(workflow, alternative, nodes, excludeEnd)
			for _, add := range toAdd {
				if !isTaskPresent(add, nodes) {
					if !isEndNode(add) || !excludeEnd {
						nodes = append(nodes, add)
					}
				}
			}
		}
		return nodes
	case *FanOutNode:
		for _, parallelBranch := range n.GetNext() {
			toAdd := Visit(workflow, parallelBranch, nodes, excludeEnd)
			for _, add := range toAdd {
				if !isTaskPresent(add, nodes) {
					if !isEndNode(add) || !excludeEnd {
						nodes = append(nodes, add)
					}
				}
			}
		}
		return nodes
	case *FanInNode:
		toAdd := Visit(workflow, n.GetNext()[0], nodes, excludeEnd)
		for _, add := range toAdd {
			if !isTaskPresent(add, nodes) {
				if !isEndNode(add) || !excludeEnd {
					nodes = append(nodes, add)
				}
			}
		}
	}
	return nodes
}

// chain can be used to connect the output of node1 to the node2
func (workflow *Workflow) chain(node1 Task, node2 Task) error {
	return node1.AddOutput(workflow, node2.GetId())
}

// ChainToEndNode (node, i) can be used as a shorthand to chain(node, workflow.end[i]) to chain a node to a specific end node
func (workflow *Workflow) ChainToEndNode(node1 Task) error {
	return workflow.chain(node1, workflow.End)
}

func (workflow *Workflow) Print() string {
	var currentNode Task = workflow.Start
	result := ""

	// prints the StartNode
	if workflow.Width == 1 {
		result += fmt.Sprintf("[%s]\n   |\n", currentNode.Name())
	} else if workflow.Width%2 == 0 {
		result += fmt.Sprintf("%s[%s]\n%s|\n", strings.Repeat(" ", 7*workflow.Width/2-3), currentNode.Name(), strings.Repeat(" ", 7*workflow.Width/2))
	} else {
		result += fmt.Sprintf("%s[%s]\n%s|\n", strings.Repeat(" ", 7*int(math.Floor(float64(workflow.Width/2)))), currentNode.Name(), strings.Repeat(" ", 7*int(math.Floor(float64(workflow.Width/2)))+3))
	}

	currentNodes := currentNode.GetNext()
	doneNodes := NewNodeSet()

	for len(currentNodes) > 0 {
		result += "["
		var currentNodesToAdd []TaskId
		for i, nodeId := range currentNodes {
			node, _ := workflow.Find(nodeId)
			result += fmt.Sprintf("%s", node.Name())

			doneNodes.AddIfNotExists(node)

			if i != len(currentNodes)-1 {
				result += "|"
			}
			var addNodes []TaskId
			switch t := node.(type) {
			case *ChoiceNode:
				addNodes = t.Alternatives
			default:
				addNodes = node.GetNext()
			}
			currentNodesToAdd = append(currentNodesToAdd, addNodes...)

		}
		result += "]\n"
		currentNodes = currentNodesToAdd
		if len(currentNodes) > 0 {
			result += strings.Repeat("   |   ", len(currentNodes)) + "\n"
		}
	}
	fmt.Println(result)
	return result
}

func (workflow *Workflow) executeStart(progress *Progress, partialData *PartialData, node *StartNode) (*PartialData, *Progress, bool, error) {

	err := progress.CompleteNode(node.GetId())
	if err != nil {
		return partialData, progress, false, err
	}
	return partialData, progress, true, nil
}

func (workflow *Workflow) executeSimple(progress *Progress, input *PartialData, task *SimpleNode, r *Request) (*PartialData, *Progress, bool, error) {

	err := task.CheckInput(input.Data)
	if err != nil {
		return nil, progress, false, err
	}
	output, err := task.Exec(r, input.Data)
	if err != nil {
		return nil, progress, false, err
	}

	errSend := task.PrepareOutput(workflow, output)
	if errSend != nil {
		return nil, progress, false, fmt.Errorf("the node %s cannot send the output2: %v", task.String(), errSend)
	}

	nextTask := task.GetNext()[0]
	outputData := NewPartialData(ReqId(r.Id), nextTask, task.Id, output)

	err = progress.CompleteNode(task.Id)
	if err != nil {
		return outputData, progress, false, err
	}

	return outputData, progress, true, nil
}

func (workflow *Workflow) executeChoice(progress *Progress, input *PartialData, task *ChoiceNode, r *Request) (*PartialData, *Progress, bool, error) {

	outputData := NewPartialData(ReqId(r.Id), "", task.GetId(), nil) // partial initialization of outputData

	// NOTE: we do not call task.CheckInput() as this task has no signature to match against

	// simply evaluate the Conditions and set the matching one
	matchedCondition := -1
	for i, condition := range task.Conditions {
		ok, err := condition.Evaluate(input.Data)
		if err != nil {
			return nil, progress, false, fmt.Errorf("error while testing condition: %v", err)
		}
		if ok {
			matchedCondition = i
			break
		}
	}

	if matchedCondition < 0 {
		return nil, progress, false, fmt.Errorf("no condition is met")
	}

	nextTaskId := task.Alternatives[matchedCondition]
	outputData.ForTask = nextTaskId
	outputData.Data = input.Data

	nextTask, ok := workflow.Find(nextTaskId)
	if !ok {
		return nil, progress, false, fmt.Errorf("node not found while preparing output")
	}
	switch typedTask := nextTask.(type) {
	case *SimpleNode:
		err := typedTask.MapOutput(outputData.Data)
		if err != nil {
			return outputData, progress, false, fmt.Errorf("choice task %s cannot prepare the output for simple task: %v", task.String(), err)
		}
	}

	// for task node, we skip all branch that will not be executed
	nodesToSkip := task.GetNodesToSkip(workflow, matchedCondition)
	errSkip := progress.SkipAll(nodesToSkip)
	if errSkip != nil {
		return outputData, progress, false, errSkip
	}

	err := progress.CompleteNode(task.GetId())
	if err != nil {
		return outputData, progress, false, err
	}

	return outputData, progress, true, nil
}

func (workflow *Workflow) executeFanOut(progress *Progress, input *PartialData, task *FanOutNode, r *Request) (*PartialData, *Progress, bool, error) {
	output, err := task.Exec(r, input.Data)
	if err != nil {
		return nil, progress, false, err
	}

	/* using forNode = "" in order to create a special input to handle fanout
	 * case with Data field which contains a map[string]interface{} with the key set
	 * to nodeId and the value which is also a map[string]interface{} containing the
	 * effective input for the nth-parallel node */
	outputData := NewPartialData(ReqId(r.Id), "", task.GetId(), output)
	//newOutputDataMap := make(map[string]interface{})        // TODO: consider using a map of PartialData rather than a single PartialData object

	// and updating progress
	err = progress.CompleteNode(task.GetId())
	if err != nil {
		return nil, progress, false, err
	}
	return outputData, progress, true, nil
}

func (workflow *Workflow) executeParallel(progress *Progress, input *PartialData, tasks []TaskId, r *Request) (*PartialData, *Progress, error) {
	// preparing workflow nodes and channels for parallel execution
	parallelTasks := make([]Task, len(tasks))
	inputs := make([]map[string]interface{}, len(tasks))
	outputChannels := make([]chan map[string]interface{}, len(tasks))
	errorChannels := make([]chan error, len(tasks))
	outputMap := make(map[string]interface{})
	// TODO: it would be enough to have a single channel, where each task sends a struct (comprising error/output and task id)

	// Populate slices of inputs and channels
	for i, taskId := range tasks {
		parallelTasks[i], _ = workflow.Find(taskId)
		outputChannels[i] = make(chan map[string]interface{})
		errorChannels[i] = make(chan error)
		inputs[i] = input.Data[fmt.Sprintf("%s", taskId)].(map[string]interface{})
	}

	// executing all nodes in parallel
	for i, task := range parallelTasks {
		go func(i int, params map[string]interface{}, currTask Task) {
			// TODO: only SimpleNode supported here!
			if simpleTask, isSimple := currTask.(*SimpleNode); isSimple {

				err := simpleTask.CheckInput(params)
				if err != nil {
					errorChannels[i] <- err
					outputChannels[i] <- nil
					return
				}
				output, err := simpleTask.Exec(r, params)
				if err != nil {
					errorChannels[i] <- err
					outputChannels[i] <- nil
					return
				}

				errSend := simpleTask.PrepareOutput(workflow, output)
				if errSend != nil {
					errorChannels[i] <- fmt.Errorf("the node %s cannot send the output2: %v", currTask.String(), errSend)
					outputChannels[i] <- nil
					return
				}

				errorChannels[i] <- nil
				outputChannels[i] <- output
			} else {
				errorChannels[i] <- fmt.Errorf("we do not support task of type %v in parallel regions", currTask.GetNodeType())
				outputChannels[i] <- nil
				return
			}
		}(i, inputs[i], task)
	}

	parallelErrors := make([]error, 0)
	for _, errChan := range errorChannels {
		err := <-errChan
		if err != nil {
			parallelErrors = append(parallelErrors, err)
			// we do not return now, because we want to quit the goroutines
			// we also need to check the outputs.
		}
	}
	if len(parallelErrors) > 0 {
		return nil, progress, fmt.Errorf("errors in parallel execution: %v", parallelErrors)
	}

	for i, outChan := range outputChannels {
		out := <-outChan
		task := parallelTasks[i]
		outputMap[fmt.Sprintf("%s", task.GetId())] = out
		err := progress.CompleteNode(parallelTasks[i].GetId())
		if err != nil {
			return nil, progress, err
		}
	}

	/* using fromNode = "" in order to create a special input to handle parallel case with
	 * Data field which contains a map[string]interface{} with the key set to taskId
	 * and the value which is also a map[string]interface{} containing the effective
	 * output of the nth-parallel task */
	outputData := NewPartialData(ReqId(r.Id), "", "", outputMap) // partial initialization of outputData
	outputData.ForTask = parallelTasks[0].GetNext()[0]           // TODO: we are assuming that the next node is unique for all the parallel tasks (i.e. a FanIn)
	return outputData, progress, nil
}

func (workflow *Workflow) executeFanIn(progress *Progress, input *PartialData, task *FanInNode, r *Request) (*PartialData, *Progress, bool, error) {
	outputData := NewPartialData(ReqId(r.Id), "", task.GetId(), nil) // partial initialization of outputData

	// TODO: are you sure it is necessary?
	//err := progress.PutInWait(task)
	//if err != nil {
	//	return false, err
	//}

	timerElapsed := false
	timer := time.AfterFunc(task.Timeout, func() {
		fmt.Println("timeout elapsed")
		timerElapsed = true
	})

	for !timerElapsed {
		if len(input.Data) == task.FanInDegree {
			break
		}
		fmt.Printf("fanin waiting partial datas: %d/%d\n", len(input.Data), task.FanInDegree)
		time.Sleep(task.Timeout / 100)
	}

	fired := timer.Stop()
	if !fired {
		return outputData, progress, false, fmt.Errorf("fan in timeout occurred")
	}
	faninInputs := make([]map[string]interface{}, 0)
	for _, partialDataMap := range input.Data {
		faninInputs = append(faninInputs, partialDataMap.(map[string]interface{}))
	}

	// merging input into one output
	output, err := task.Exec(r, faninInputs...)
	if err != nil {
		return outputData, progress, false, err
	}

	// setting the remaining field of outputData
	outputData.ForTask = task.GetNext()[0]
	outputData.Data = output
	err = progress.CompleteNode(task.GetId())
	if err != nil {
		return outputData, progress, false, err
	}

	return outputData, progress, true, nil
}

func (workflow *Workflow) doNothingExec(progress *Progress, input *PartialData, task Task, r *Request) (*PartialData, *Progress, bool, error) {

	forNode := task.GetNext()[0]
	output := input.Data
	outputData := NewPartialData(ReqId(r.Id), forNode, task.GetId(), output)

	err := progress.CompleteNode(task.GetId())
	if err != nil {
		return outputData, progress, false, err
	}

	shouldContinueExecution := task.GetNodeType() != Fail && task.GetNodeType() != Succeed
	return outputData, progress, shouldContinueExecution, nil
}

func (workflow *Workflow) executeFail(progress *Progress, task *FailNode, r *Request) (*PartialData, *Progress, bool, error) {

	forNode := task.GetNext()[0]
	output := make(map[string]interface{})
	output[task.Error] = task.Cause
	outputData := NewPartialData(ReqId(r.Id), forNode, task.GetId(), output)

	err := progress.CompleteNode(task.GetId())
	if err != nil {
		return outputData, progress, false, err
	}

	shouldContinueExecution := task.GetNodeType() != Fail && task.GetNodeType() != Succeed
	return outputData, progress, shouldContinueExecution, nil
}

func (workflow *Workflow) executeEnd(progress *Progress, partialData *PartialData, node *EndNode, r *Request) (*PartialData, *Progress, bool, error) {
	err := progress.CompleteNode(node.Id)
	if err != nil {
		return partialData, progress, false, err
	}
	return partialData, progress, false, nil // false because we want to stop when reaching the end
}

func (workflow *Workflow) Execute(r *Request, input *PartialData, progress *Progress) (*PartialData, *Progress, bool, error) {

	var output *PartialData
	nextNodes, err := progress.NextNodes()
	if err != nil {
		return nil, progress, false, fmt.Errorf("failed to get next nodes from progress: %v", err)
	}
	shouldContinue := true

	if len(nextNodes) > 1 {
		// TODO: check executeParallel
		output, progress, err = workflow.executeParallel(progress, input, nextNodes, r)
		if err != nil {
			return output, progress, true, err
		}
	} else if len(nextNodes) == 1 {
		n, ok := workflow.Find(nextNodes[0])
		if !ok {
			return nil, progress, true, fmt.Errorf("failed to find node %s", n.GetId())
		}

		switch node := n.(type) {
		case *SimpleNode:
			output, progress, shouldContinue, err = workflow.executeSimple(progress, input, node, r)
		case *ChoiceNode:
			output, progress, shouldContinue, err = workflow.executeChoice(progress, input, node, r)
		case *FanInNode:
			output, progress, shouldContinue, err = workflow.executeFanIn(progress, input, node, r)
		case *StartNode:
			output, progress, shouldContinue, err = workflow.executeStart(progress, input, node)
		case *FanOutNode:
			output, progress, shouldContinue, err = workflow.executeFanOut(progress, input, node, r)
		case *PassNode:
			output, progress, shouldContinue, err = workflow.doNothingExec(progress, input, node, r)
		case *FailNode:
			output, progress, shouldContinue, err = workflow.executeFail(progress, node, r)
		case *SucceedNode:
			output, progress, shouldContinue, err = workflow.doNothingExec(progress, input, node, r)
		case *EndNode:
			output, progress, shouldContinue, err = workflow.executeEnd(progress, input, node, r)
		}
		if err != nil {
			_ = progress.FailNode(n.GetId())
			r.ExecReport.Progress = progress
			return output, progress, false, err
		}
	} else {
		// should never happen
		return nil, progress, false, nil
	}

	return output, progress, shouldContinue, nil
}

// GetUniqueFunctions returns a list with the function names used in the Workflow. The returned function names are unique and in alphabetical order
func (workflow *Workflow) GetUniqueFunctions() []string {
	allFunctionsMap := make(map[string]void)
	for _, node := range workflow.Nodes {
		switch n := node.(type) {
		case *SimpleNode:
			allFunctionsMap[n.Func] = null
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
func (workflow *Workflow) Invoke(r *Request) (ExecutionReport, error) {

	var err error
	requestId := ReqId(r.Id)

	// TODO: check these functions
	progress := InitProgressRecursive(requestId, workflow)
	pd := NewPartialData(requestId, workflow.Start.Next, "nil", r.Params) // TODO: can we use an empty string rather than "nil"?

	shouldContinue := true
	for shouldContinue {
		// executing workflow
		pd, progress, shouldContinue, err = workflow.Execute(r, pd, progress)
		if err != nil {
			return ExecutionReport{Result: nil, Progress: progress}, fmt.Errorf("failed workflow execution: %v", err)
		}
	}

	// TODO: we may need to handle offloading decisions here
	//// saving partialData and progress on etcd - implementing workflow offloading policies
	//err = savePartialDataToEtcd(pd)
	//if err != nil {
	//	return ExecutionReport{}, err
	//}
	//err = saveProgressToEtcd(progress)
	//if err != nil {
	//	return ExecutionReport{}, err
	//}

	//// deleting progresses and partial datas from cache and etcd
	//err = DeleteProgress(requestId, cache.Persist)
	//if err != nil {
	//	return ExecutionReport{}, err
	//}
	//_, errDel := DeleteAllPartialData(requestId, cache.Persist)
	//if errDel != nil {
	//	return ExecutionReport{}, errDel
	//}
	r.ExecReport.Result = pd.Data

	return r.ExecReport, nil
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
				log.Error(err.Error())
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

	for k := range workflow.Nodes {
		if !workflow.Nodes[k].Equals(workflow2.Nodes[k]) {
			return false
		}
	}
	return workflow.Start.Equals(workflow2.Start) &&
		workflow.End.Equals(workflow2.End) &&
		workflow.Width == workflow2.Width &&
		len(workflow.Nodes) == len(workflow2.Nodes)
}

func (workflow *Workflow) String() string {
	return fmt.Sprintf(`Workflow{
		Name: %s,
		Start: %s,
		Nodes: %s,
		End:   %s,
		Width: %d,
	}`, workflow.Name, workflow.Start.String(), workflow.Nodes, workflow.End.String(), workflow.Width)
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
	data["Width"] = workflow.Width
	nodes := make(map[TaskId]interface{})

	// Marshal the interface and store it as concrete node value in the map
	for nodeId, node := range workflow.Nodes {
		nodes[nodeId] = node
	}
	data["Nodes"] = nodes

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

	if rawWidth, ok := tempMap["Width"]; ok {
		if err := json.Unmarshal(rawWidth, &workflow.Width); err != nil {
			return err
		}
	} else {
		return fmt.Errorf("missing 'Width' field in JSON")
	}

	if rawName, ok := tempMap["Name"]; ok {
		if err := json.Unmarshal(rawName, &workflow.Name); err != nil {
			return err
		}
	} else {
		return fmt.Errorf("missing 'Name' field in JSON")
	}

	// Cycle on each map entry and decode the type
	var tempNodeMap map[string]json.RawMessage
	if err := json.Unmarshal(tempMap["Nodes"], &tempNodeMap); err != nil {
		return err
	}
	workflow.Nodes = make(map[TaskId]Task)
	for nodeId, value := range tempNodeMap {
		err := workflow.decodeNode(nodeId, value)
		if err != nil {
			return err
		}
	}
	return nil
}

func (workflow *Workflow) decodeNode(nodeId string, value json.RawMessage) error {
	var tempNodeMap map[string]interface{}
	if err := json.Unmarshal(value, &tempNodeMap); err != nil {
		return err
	}
	taskType, ok := tempNodeMap["NodeType"].(string)
	if !ok {
		return fmt.Errorf("unknown nodeType: %v", tempNodeMap["NodeType"])
	}
	var err error

	node := TaskFromType(TaskType(taskType))

	switch TaskType(taskType) {
	case Start:
		node := &StartNode{}
		err = json.Unmarshal(value, node)
		if err == nil && node.Id != "" && node.Next != "" {
			workflow.Nodes[TaskId(nodeId)] = node
			return nil
		}
	case Simple:
		node := &SimpleNode{}
		err = json.Unmarshal(value, node)
		if err == nil && node.Id != "" && node.Func != "" {
			workflow.Nodes[TaskId(nodeId)] = node
			return nil
		}
	case Choice:
		node := &ChoiceNode{}
		err = json.Unmarshal(value, node)
		if err == nil && node.Id != "" && node.Alternatives != nil && len(node.Alternatives) == len(node.Conditions) {
			workflow.Nodes[TaskId(nodeId)] = node
			return nil
		}
	default:
		err = json.Unmarshal(value, node)
		if err == nil && node.GetId() != "" {
			workflow.Nodes[TaskId(nodeId)] = node
			return nil
		}
	}
	var unmarshalTypeError *json.UnmarshalTypeError
	if err != nil && !errors.As(err, &unmarshalTypeError) {
		// abort if we have an error other than the wrong type
		return err
	}

	return fmt.Errorf("failed to decode node")
}

// IsEmpty returns true if the workflow has 0 nodes or exactly one StartNode and one EndNode.
func (workflow *Workflow) IsEmpty() bool {
	if len(workflow.Nodes) == 0 {
		return true
	}

	onlyTwoNodes := len(workflow.Nodes) == 2
	hasOnlyStartAndEnd := false
	if onlyTwoNodes {
		hasStart := 0
		hasEnd := 0
		for _, node := range workflow.Nodes {
			if node.GetNodeType() == Start {
				hasStart++
			}
			if node.GetNodeType() == End {
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
