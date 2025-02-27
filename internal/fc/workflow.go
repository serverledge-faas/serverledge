package fc

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

// Workflow is a Workflow to drive the execution of the function composition
type Workflow struct {
	Name  string     // identifier of the Workflow
	Start *StartNode // a single start must be added
	Nodes map[TaskId]Task
	End   *EndNode // a single endNode must be added
	Width int      // width is the max fanOut degree of the Workflow
}

func NewWorkflowWithName(name string) Workflow {
	workflow := NewWorkflow()
	workflow.Name = name
	return workflow
}

// TODO: handle Workflow creation with name better
func NewWorkflow() Workflow {
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

// TODO: only the subsequent APIs should be public: NewDag, Print, GetUniqueFunctions, Equals
//  the remaining should be private after the builder APIs work well!!!

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
	case *SimpleNode, *PassNode, *WaitNode, *SucceedNode, *FailNode:
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

func (workflow *Workflow) executeStart(progress *Progress, partialData *PartialData, node *StartNode, r *CompositionRequest) (*PartialData, *Progress, bool, error) {

	err := progress.CompleteNode(node.GetId())
	if err != nil {
		return partialData, progress, false, err
	}
	r.ExecReport.Reports.Set(CreateExecutionReportId(node), &function.ExecutionReport{Result: "start"})
	return partialData, progress, true, nil
}

func (workflow *Workflow) executeSimple(progress *Progress, partialData *PartialData, simpleNode *SimpleNode, r *CompositionRequest) (*PartialData, *Progress, bool, error) {
	// retrieving input
	var pd *PartialData
	nodeId := simpleNode.GetId()
	requestId := ReqId(r.ReqId)
	pd = NewPartialData(requestId, "", nodeId, nil) // partial initialization of pd

	err := simpleNode.CheckInput(partialData.Data)
	if err != nil {
		return pd, progress, false, err
	}
	// executing node
	output, err := simpleNode.Exec(r, partialData.Data)
	if err != nil {
		return pd, progress, false, err
	}

	// Todo: uncomment when running TestInvokeFC_Concurrent to debug concurrency errors
	// errDbg := Debug(r, string(simpleNode.Id), output)
	// if errDbg != nil {
	// 	return false, errDbg
	// }

	forNode := simpleNode.GetNext()[0]

	errSend := simpleNode.PrepareOutput(workflow, output)
	if errSend != nil {
		return pd, progress, false, fmt.Errorf("the node %s cannot send the output: %v", simpleNode.String(), errSend)
	}

	// setting the remaining fields of pd
	pd.ForNode = forNode
	pd.Data = output

	err = progress.CompleteNode(nodeId)
	if err != nil {
		return pd, progress, false, err
	}

	return pd, progress, true, nil
}

func (workflow *Workflow) executeChoice(progress *Progress, partialData *PartialData, choice *ChoiceNode, r *CompositionRequest) (*PartialData, *Progress, bool, error) {

	var pd *PartialData
	nodeId := choice.GetId()
	requestId := ReqId(r.ReqId)
	pd = NewPartialData(requestId, "", nodeId, nil) // partial initialization of pd

	err := choice.CheckInput(partialData.Data)
	if err != nil {
		return pd, progress, false, err
	}
	// executing node
	output, err := choice.Exec(r, partialData.Data)
	if err != nil {
		return pd, progress, false, err
	}

	// setting the remaining fields of pd
	pd.ForNode = choice.GetNext()[0]
	pd.Data = output

	errSend := choice.PrepareOutput(workflow, output)
	if errSend != nil {
		return pd, progress, false, fmt.Errorf("the node %s cannot send the output: %v", choice.String(), errSend)
	}

	// for choice node, we skip all branch that will not be executed
	nodesToSkip := choice.GetNodesToSkip(workflow)
	errSkip := progress.SkipAll(nodesToSkip)
	if errSkip != nil {
		return pd, progress, false, errSkip
	}

	err = progress.CompleteNode(nodeId)
	if err != nil {
		return pd, progress, false, err
	}

	return pd, progress, true, nil
}

func (workflow *Workflow) executeFanOut(progress *Progress, partialData *PartialData, fanOut *FanOutNode, r *CompositionRequest) (*PartialData, *Progress, bool, error) {

	var pd *PartialData
	outputMap := make(map[string]interface{})
	nodeId := fanOut.GetId()
	requestId := ReqId(r.ReqId)

	/* using forNode = "" in order to create a special partialData to handle fanout
	 * case with Data field which contains a map[string]interface{} with the key set
	 * to nodeId and the value which is also a map[string]interface{} containing the
	 * effective input for the nth-parallel node */
	pd = NewPartialData(requestId, "", nodeId, nil) // partial initialization of pd

	// executing node
	output, err := fanOut.Exec(r, partialData.Data)
	if err != nil {
		return pd, progress, false, err
	}
	// sends output to each next node
	errSend := fanOut.PrepareOutput(workflow, output)
	if errSend != nil {
		return pd, progress, false, fmt.Errorf("the node %s cannot send the output: %v", fanOut.String(), errSend)
	}

	for i, nextNode := range fanOut.GetNext() {
		if fanOut.Type == Broadcast {
			outputMap[fmt.Sprintf("%s", nextNode)] = output[fmt.Sprintf("%d", i)].(map[string]interface{})
		} else if fanOut.Type == Scatter {
			firstName := ""
			for name := range output {
				firstName = name
				break
			}
			inputForNode := make(map[string]interface{})
			subMap, found := output[firstName].(map[string]interface{})
			if !found {
				return pd, progress, false, fmt.Errorf("cannot find parameter for nextNode %s", nextNode)
			}
			inputForNode[firstName] = subMap[fmt.Sprintf("%d", i)]
			outputMap[fmt.Sprintf("%s", nextNode)] = inputForNode
		} else {
			return pd, progress, false, fmt.Errorf("invalid fanout type %d", fanOut.Type)
		}
	}

	// setting the remaining field of pd
	pd.Data = outputMap
	// and updating progress
	err = progress.CompleteNode(nodeId)
	if err != nil {
		return pd, progress, false, err
	}
	return pd, progress, true, nil
}

func (workflow *Workflow) executeParallel(progress *Progress, partialData *PartialData, nextNodes []TaskId, r *CompositionRequest) (*PartialData, *Progress, error) {
	// preparing workflow nodes and channels for parallel execution
	parallelTasks := make([]Task, 0)
	inputs := make([]map[string]interface{}, 0)
	outputChannels := make([]chan map[string]interface{}, 0)
	errorChannels := make([]chan error, 0)
	requestId := ReqId(r.ReqId)
	outputMap := make(map[string]interface{}, 0)
	var node Task
	pd := NewPartialData(requestId, "", "", nil) // partial initialization of pd

	for _, nodeId := range nextNodes {
		node, ok := workflow.Find(nodeId)
		if ok {
			parallelTasks = append(parallelTasks, node)
			outputChannels = append(outputChannels, make(chan map[string]interface{}))
			errorChannels = append(errorChannels, make(chan error))
		}
		// for simple node we also retrieve the partial data and receive input
		if simple, isSimple := node.(*SimpleNode); isSimple {
			errInput := simple.CheckInput(partialData.Data[fmt.Sprintf("%s", nodeId)].(map[string]interface{}))
			if errInput != nil {
				return pd, progress, errInput
			}
			inputs = append(inputs, partialData.Data[fmt.Sprintf("%s", nodeId)].(map[string]interface{}))
		}
	}
	// executing all nodes in parallel
	for i, node := range parallelTasks {
		go func(i int, params map[string]interface{}, node Task) {
			output, err := node.Exec(r, params)
			// for simple node, we also prepare output
			if simpleNode, isSimple := node.(*SimpleNode); isSimple {
				errSend := simpleNode.PrepareOutput(workflow, output)
				if errSend != nil {
					errorChannels[i] <- err
					outputChannels[i] <- nil
					return
				}
			}
			// first send on error, then on output channels
			if err != nil {
				errorChannels[i] <- err
				outputChannels[i] <- nil
				return
			}
			errorChannels[i] <- nil
			outputChannels[i] <- output
		}(i, inputs[i], node)
	}
	// checking errors
	parallelErrors := make([]error, 0)
	for _, errChan := range errorChannels {
		err := <-errChan
		if err != nil {
			parallelErrors = append(parallelErrors, err)
			// we do not return now, because we want to quit the goroutines
			// we also need to check the outputs.
		}
	}
	// retrieving outputs (goroutines should end now)
	parallelOutputs := make([]map[string]interface{}, 0)
	for _, outChan := range outputChannels {
		out := <-outChan
		if out != nil {
			parallelOutputs = append(parallelOutputs, out)
		}
	}
	// returning errors
	if len(parallelErrors) > 0 {
		return pd, progress, fmt.Errorf("errors in parallel execution: %v", parallelErrors)
	}

	for i, output := range parallelOutputs {
		node = parallelTasks[i]
		outputMap[fmt.Sprintf("%s", node.GetId())] = output
		err := progress.CompleteNode(parallelTasks[i].GetId())
		if err != nil {
			return pd, progress, err
		}
	}
	/* using fromNode = "" in order to create a special partialData to handle parallel case with
	 * Data field which contains a map[string]interface{} with the key set to nodeId
	 * and the value which is also a map[string]interface{} containing the effective
	 * output of the nth-parallel node */
	//pd := NewPartialData(requestId, node.GetNext()[0], "", outputMap)
	// setting the remaining fields of pd
	// TODO: node is updated within the previous loop, hence we only get the next node for 1 of the parallel nodes
	pd.ForNode = node.GetNext()[0]
	pd.Data = outputMap
	return pd, progress, nil
}

func (workflow *Workflow) executeFanIn(progress *Progress, partialData *PartialData, fanIn *FanInNode, r *CompositionRequest) (*PartialData, *Progress, bool, error) {
	nodeId := fanIn.GetId()
	requestId := ReqId(r.ReqId)
	var err error
	pd := NewPartialData(requestId, "", nodeId, nil) // partial initialization of pd

	// TODO: are you sure it is necessary?
	//err := progress.PutInWait(fanIn)
	//if err != nil {
	//	return false, err
	//}

	timerElapsed := false
	timer := time.AfterFunc(fanIn.Timeout, func() {
		fmt.Println("timeout elapsed")
		timerElapsed = true
	})

	for !timerElapsed {
		if len(partialData.Data) == fanIn.FanInDegree {
			break
		}
		fmt.Printf("fanin waiting partial datas: %d/%d\n", len(partialData.Data), fanIn.FanInDegree)
		time.Sleep(fanIn.Timeout / 100)
	}

	fired := timer.Stop()
	if !fired {
		return pd, progress, false, fmt.Errorf("fan in timeout occurred")
	}
	faninInputs := make([]map[string]interface{}, 0)
	for _, partialDataMap := range partialData.Data {
		faninInputs = append(faninInputs, partialDataMap.(map[string]interface{}))
	}

	// merging input into one output
	output, err := fanIn.Exec(r, faninInputs...)
	if err != nil {
		return pd, progress, false, err
	}

	// setting the remaining field of pd
	pd.ForNode = fanIn.GetNext()[0]
	pd.Data = output
	err = progress.CompleteNode(nodeId)
	if err != nil {
		return pd, progress, false, err
	}

	return pd, progress, true, nil
}

func (workflow *Workflow) executeSucceedNode(progress *Progress, partialData *PartialData, succeedNode *SucceedNode, r *CompositionRequest) (*PartialData, *Progress, bool, error) {
	return commonExec(workflow, progress, partialData, succeedNode, r)
}

func (workflow *Workflow) executeFailNode(progress *Progress, partialData *PartialData, failNode *FailNode, r *CompositionRequest) (*PartialData, *Progress, bool, error) {
	return commonExec(workflow, progress, partialData, failNode, r)
}

func commonExec(workflow *Workflow, progress *Progress, partialData *PartialData, node Task, r *CompositionRequest) (*PartialData, *Progress, bool, error) {
	var pd *PartialData
	nodeId := node.GetId()
	requestId := ReqId(r.ReqId)
	pd = NewPartialData(requestId, "", nodeId, nil) // partial initialization of pd

	err := node.CheckInput(partialData.Data)
	if err != nil {
		return pd, progress, false, err
	}
	// executing node
	output, err := node.Exec(r, partialData.Data)
	if err != nil {
		return pd, progress, false, err
	}

	// Todo: uncomment when running TestInvokeFC_Concurrent to debug concurrency errors
	// errDbg := Debug(r, string(node.Id), output)
	// if errDbg != nil {
	// 	return false, errDbg
	// }

	errSend := node.PrepareOutput(workflow, output)
	if errSend != nil {
		return pd, progress, false, fmt.Errorf("the node %s cannot send the output: %v", node.String(), errSend)
	}

	forNode := node.GetNext()[0]
	// setting the remaining fields of pd
	pd.ForNode = forNode
	pd.Data = output

	err = progress.CompleteNode(nodeId)
	if err != nil {
		return pd, progress, false, err
	}
	if node.GetNodeType() == Fail || node.GetNodeType() == Succeed {
		return pd, progress, false, nil
	}
	return pd, progress, true, nil
}

func (workflow *Workflow) executeEnd(progress *Progress, partialData *PartialData, node *EndNode, r *CompositionRequest) (*PartialData, *Progress, bool, error) {
	r.ExecReport.Reports.Set(CreateExecutionReportId(node), &function.ExecutionReport{Result: "end"})
	err := progress.CompleteNode(node.Id)
	if err != nil {
		return partialData, progress, false, err
	}
	return partialData, progress, false, nil // false because we want to stop when reaching the end
}

func (workflow *Workflow) Execute(r *CompositionRequest, data *PartialData, progress *Progress) (*PartialData, *Progress, bool, error) {

	var pd *PartialData
	nextNodes, err := progress.NextNodes()
	if err != nil {
		return data, progress, false, fmt.Errorf("failed to get next nodes from progress: %v", err)
	}
	shouldContinue := true

	if len(nextNodes) > 1 {
		pd, progress, err = workflow.executeParallel(progress, data, nextNodes, r)
		if err != nil {
			return pd, progress, true, err
		}
	} else if len(nextNodes) == 1 {
		n, ok := workflow.Find(nextNodes[0])
		if !ok {
			return data, progress, true, fmt.Errorf("failed to find node %s", n.GetId())
		}

		switch node := n.(type) {
		case *SimpleNode:
			pd, progress, shouldContinue, err = workflow.executeSimple(progress, data, node, r)
		case *ChoiceNode:
			pd, progress, shouldContinue, err = workflow.executeChoice(progress, data, node, r)
		case *FanInNode:
			pd, progress, shouldContinue, err = workflow.executeFanIn(progress, data, node, r)
		case *StartNode:
			pd, progress, shouldContinue, err = workflow.executeStart(progress, data, node, r)
		case *FanOutNode:
			pd, progress, shouldContinue, err = workflow.executeFanOut(progress, data, node, r)
		case *PassNode:
			pd, progress, shouldContinue, err = commonExec(workflow, progress, data, node, r)
		case *WaitNode:
			pd, progress, shouldContinue, err = commonExec(workflow, progress, data, node, r)
		case *FailNode:
			pd, progress, shouldContinue, err = workflow.executeFailNode(progress, data, node, r) // TODO: use commonExec
		case *SucceedNode:
			pd, progress, shouldContinue, err = workflow.executeSucceedNode(progress, data, node, r) // TODO: use commonExec
		case *EndNode:
			pd, progress, shouldContinue, err = workflow.executeEnd(progress, data, node, r)
		}
		if err != nil {
			_ = progress.FailNode(n.GetId())
			r.ExecReport.Progress = progress
			return pd, progress, true, err
		}
	} else {
		return data, progress, false, fmt.Errorf("there aren't next nodes")
	}

	return pd, progress, shouldContinue, nil
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
	return fmt.Sprintf("/fc/%s", workflowName)
}

// GetAllFC returns the function composition names
func GetAllFC() ([]string, error) {
	return function.GetAllWithPrefix("/fc")
}

func getFCFromCache(name string) (*Workflow, bool) {
	localCache := cache.GetCacheInstance()
	cachedObj, found := localCache.Get(name)
	if !found {
		return nil, false
	}
	//cache hit
	//return a safe copy of the function composition previously obtained
	fc := *cachedObj.(*Workflow)
	return &fc, true
}

func getFCFromEtcd(name string) (*Workflow, error) {
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

// GetFC gets the Workflow from cache or from ETCD
func GetFC(name string) (*Workflow, bool) {
	val, found := getFCFromCache(name)
	if !found {
		// cache miss
		f, err := getFCFromEtcd(name)
		if err != nil {
			return nil, false
		}
		//insert a new element to the cache
		cache.GetCacheInstance().Set(name, f, cache.DefaultExp)
		return f, true
	}

	return val, true
}

// SaveToEtcd creates and register the function composition in Serverledge
// It is like SaveToEtcd for a simple function
func (workflow *Workflow) SaveToEtcd() error {
	if len(workflow.Name) == 0 {
		return fmt.Errorf("cannot save an anonymous workflow (no name set)")
	}

	cli, err := utils.GetEtcdClient()
	if err != nil {
		return err
	}
	ctx := context.TODO()

	// marshal the function composition object into json
	payload, err := json.Marshal(*workflow)
	if err != nil {
		return fmt.Errorf("could not marshal function composition: %v", err)
	}
	// saves the json object into etcd
	_, err = cli.Put(ctx, workflow.getEtcdKey(), string(payload))
	if err != nil {
		return fmt.Errorf("failed etcd Put: %v", err)
	}

	// Add the function composition to the local cache
	cache.GetCacheInstance().Set(workflow.Name, workflow, cache.DefaultExp)

	return nil
}

// Invoke schedules each function of the composition and invokes them
func (workflow *Workflow) Invoke(r *CompositionRequest) (CompositionExecutionReport, error) {

	var err error
	requestId := ReqId(r.ReqId)
	input := r.Params
	// initialize struct progress from workflow
	progress := InitProgressRecursive(requestId, workflow)

	// initialize partial data with input, directly from the Start.Next node
	pd := NewPartialData(requestId, workflow.Start.Next, "nil", input)
	pd.Data = input

	shouldContinue := true
	for shouldContinue {
		// executing workflow
		pd, progress, shouldContinue, err = workflow.Execute(r, pd, progress)
		if err != nil {
			return CompositionExecutionReport{Result: nil, Progress: progress}, fmt.Errorf("failed workflow execution: %v", err)
		}
	}

	// saving partialData and progress on etcd - implementing workflow offloading policies
	err = savePartialDataToEtcd(pd)
	if err != nil {
		return CompositionExecutionReport{}, err
	}
	err = saveProgressToEtcd(progress)
	if err != nil {
		return CompositionExecutionReport{}, err
	}

	// deleting progresses and partial datas from cache and etcd
	err = DeleteProgress(requestId, cache.Persist)
	if err != nil {
		return CompositionExecutionReport{}, err
	}
	_, errDel := DeleteAllPartialData(requestId, cache.Persist)
	if errDel != nil {
		return CompositionExecutionReport{}, errDel
	}
	r.ExecReport.Result = pd.Data

	return r.ExecReport, nil
}

// Delete removes the FunctionComposition from cache and from etcd, so it cannot be invoked anymore
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

// Exists return true if the function composition exists either in etcd or in cache. If it only exists in Etcd, it saves the composition also in caches
func (workflow *Workflow) Exists() bool {
	_, found := getFCFromCache(workflow.Name)
	if !found {
		// cache miss
		f, err := getFCFromEtcd(workflow.Name)
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

func WorkflowBuildingLoop(sm *asl.StateMachine, nextState asl.State, nextStateName string) (*Workflow, error) {
	builder := NewBuilder()
	isTerminal := false
	// forse questo va messo in un metodo a parte e riutilizzato per navigare i branch dei choice
	for !isTerminal {

		switch nextState.GetType() {
		case asl.Task:

			taskState := nextState.(*asl.TaskState)
			b, err := BuildFromTaskState(builder, taskState, nextStateName)
			if err != nil {
				return nil, fmt.Errorf("failed building SimpleNode from task state: %v", err)
			}
			builder = b
			nextState, nextStateName, isTerminal = findNextOrTerminate(taskState, sm)
			break
		case asl.Parallel:
			parallelState := nextState.(*asl.ParallelState)
			b, err := BuildFromParallelState(builder, parallelState, nextStateName)
			if err != nil {
				return nil, fmt.Errorf("failed building FanInNode and FanOutNode from ParallelState: %v", err)
			}
			builder = b
			nextState, nextStateName, isTerminal = findNextOrTerminate(parallelState, sm)
			break
		case asl.Map:
			mapState := nextState.(*asl.MapState)
			b, err := BuildFromMapState(builder, mapState, nextStateName)
			if err != nil {
				return nil, fmt.Errorf("failed building MapNode from Map state: %v", err) // TODO: MapNode doesn't exist
			}
			builder = b
			nextState, nextStateName, isTerminal = findNextOrTerminate(mapState, sm)
			break
		case asl.Pass:
			passState := nextState.(*asl.PassState)
			b, err := BuildFromPassState(builder, passState, nextStateName)
			if err != nil {
				return nil, fmt.Errorf("failed building SimplNode with function 'pass' from Pass state: %v", err)
			}
			builder = b
			nextState, nextStateName, isTerminal = findNextOrTerminate(passState, sm)
			break
		case asl.Wait:
			waitState := nextState.(*asl.WaitState)
			b, err := BuildFromWaitState(builder, waitState, nextStateName)
			if err != nil {
				return nil, fmt.Errorf("failed building SimpleNode with function 'wait' from Wait state: %v", err)
			}
			builder = b
			nextState, nextStateName, isTerminal = findNextOrTerminate(waitState, sm)
			break
		case asl.Choice:
			choiceState := nextState.(*asl.ChoiceState)
			// In this case, the choice state will automatically build the workflow, because it is terminal
			return BuildFromChoiceState(builder, choiceState, nextStateName, sm)
		case asl.Succeed:
			succeed := nextState.(*asl.SucceedState)
			return BuildFromSucceedState(builder, succeed, nextStateName)
		case asl.Fail:
			failState := nextState.(*asl.FailState)
			return BuildFromFailState(builder, failState, nextStateName)
		default:
			return nil, fmt.Errorf("unknown state type %s", nextState.GetType())
		}
	}
	return builder.Build()
}

func FromStateMachine(sm *asl.StateMachine) (*Workflow, error) {
	nextStateName := sm.StartAt
	nextState := sm.States[nextStateName]
	return WorkflowBuildingLoop(sm, nextState, nextStateName)
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
