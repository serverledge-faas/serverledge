package fc

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/grussorusso/serverledge/internal/cache"
	"github.com/grussorusso/serverledge/internal/function"
	"github.com/grussorusso/serverledge/internal/types"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"
)

// used to send output from parallel nodes to fan in node or to the next node
// var outputChannel = make(chan map[string]interface{})

// Dag is a Workflow to drive the execution of the function composition
type Dag struct {
	Start *StartNode // a single start must be added
	Nodes map[DagNodeId]DagNode
	End   *EndNode // a single endNode must be added
	Width int      // width is the max fanOut degree of the Dag
}

func NewDAG() Dag {
	start := NewStartNode()
	end := NewEndNode()
	nodes := make(map[DagNodeId]DagNode)
	nodes[start.Id] = start
	nodes[end.Id] = end

	dag := Dag{
		Start: start,
		End:   end,
		Nodes: nodes,
		Width: 1,
	}
	return dag
}

func (dag *Dag) Find(nodeId DagNodeId) (DagNode, bool) {
	dagNode, found := dag.Nodes[nodeId]
	return dagNode, found
}

// TODO: only the subsequent APIs should be public: NewDag, Print, GetUniqueDagFunctions, Equals
//  the remaining should be private after the builder APIs work well!!!

// addNode can be used to add a new node to the Dag. Does not chain anything, but updates Dag width
func (dag *Dag) addNode(node DagNode) {
	dag.Nodes[node.GetId()] = node // if already exists, overwrites!
	// updates width
	nodeWidth := node.Width()
	if nodeWidth > dag.Width {
		dag.Width = nodeWidth
	}
}

func isDagNodePresent(node DagNode, infos []DagNode) bool {
	isPresent := false
	for _, nodeInfo := range infos {
		if nodeInfo == node {
			isPresent = true
			break
		}
	}
	return isPresent
}

func isEndNode(node DagNode) bool {
	_, ok := node.(*EndNode)
	return ok
}

// VisitDag visits the dag starting from the input node and return a list of visited nodes. If excludeEnd = true, the EndNode will not be in the output list
func VisitDag(dag *Dag, nodeId DagNodeId, nodes []DagNode, excludeEnd bool) []DagNode {
	node, ok := dag.Find(nodeId)
	if !ok {
		return []DagNode{}
	}
	if !isDagNodePresent(node, nodes) {
		nodes = append(nodes, node)
	}
	switch n := node.(type) {
	case *StartNode:
		toAdd := VisitDag(dag, n.GetNext()[0], nodes, excludeEnd)
		for _, add := range toAdd {
			if !isDagNodePresent(add, nodes) {
				// only when isEndNode = true, excludeEnd = true -> we don't add the node
				if !isEndNode(add) || !excludeEnd {
					nodes = append(nodes, add)
				}
			}
		}
		return nodes
	case *SimpleNode:
		toAdd := VisitDag(dag, n.GetNext()[0], nodes, excludeEnd)
		for _, add := range toAdd {
			if !isDagNodePresent(add, nodes) {
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
			for i, dagNode := range nodes {
				if isEndNode(dagNode) {
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
			toAdd := VisitDag(dag, alternative, nodes, excludeEnd)
			for _, add := range toAdd {
				if !isDagNodePresent(add, nodes) {
					if !isEndNode(add) || !excludeEnd {
						nodes = append(nodes, add)
					}
				}
			}
		}
		return nodes
	case *FanOutNode:
		for _, parallelBranch := range n.GetNext() {
			toAdd := VisitDag(dag, parallelBranch, nodes, excludeEnd)
			for _, add := range toAdd {
				if !isDagNodePresent(add, nodes) {
					if !isEndNode(add) || !excludeEnd {
						nodes = append(nodes, add)
					}
				}
			}
		}
		return nodes
	case *FanInNode:
		toAdd := VisitDag(dag, n.GetNext()[0], nodes, excludeEnd)
		for _, add := range toAdd {
			if !isDagNodePresent(add, nodes) {
				if !isEndNode(add) || !excludeEnd {
					nodes = append(nodes, add)
				}
			}
		}
	}
	return nodes
}

// chain can be used to connect the output of node1 to the node2
func (dag *Dag) chain(node1 DagNode, node2 DagNode) error {
	return node1.AddOutput(dag, node2.GetId())
}

// ChainToEndNode (node, i) can be used as a shorthand to chain(node, dag.end[i]) to chain a node to a specific end node
func (dag *Dag) ChainToEndNode(node1 DagNode) error {
	return dag.chain(node1, dag.End)
}

func (dag *Dag) Print() string {
	var currentNode DagNode = dag.Start
	result := ""

	// prints the StartNode
	if dag.Width == 1 {
		result += fmt.Sprintf("[%s]\n   |\n", currentNode.Name())
	} else if dag.Width%2 == 0 {
		result += fmt.Sprintf("%s[%s]\n%s|\n", strings.Repeat(" ", 7*dag.Width/2-3), currentNode.Name(), strings.Repeat(" ", 7*dag.Width/2))
	} else {
		result += fmt.Sprintf("%s[%s]\n%s|\n", strings.Repeat(" ", 7*int(math.Floor(float64(dag.Width/2)))), currentNode.Name(), strings.Repeat(" ", 7*int(math.Floor(float64(dag.Width/2)))+3))
	}

	currentNodes := currentNode.GetNext()
	doneNodes := NewNodeSet()

	for len(currentNodes) > 0 {
		result += "["
		var currentNodesToAdd []DagNodeId
		for i, nodeId := range currentNodes {
			node, _ := dag.Find(nodeId)
			result += fmt.Sprintf("%s", node.Name())

			doneNodes.AddIfNotExists(node)

			if i != len(currentNodes)-1 {
				result += "|"
			}
			var addNodes []DagNodeId
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

func (dag *Dag) executeStart(progress *Progress, node *StartNode, r *CompositionRequest) (bool, error) {
	err := progress.CompleteNode(node.GetId())
	if err != nil {
		return false, err
	}
	//r.ExecReport.Reports[CreateExecutionReportId(node)] = &function.ExecutionReport{Result: "start"}
	r.ExecReport.Reports.Set(CreateExecutionReportId(node), &function.ExecutionReport{Result: "start"})
	return true, nil
}

func (dag *Dag) executeSimple(progress *Progress, simpleNode *SimpleNode, r *CompositionRequest) (bool, error) {
	// retrieving input
	var pd *PartialData
	nodeId := simpleNode.GetId()
	requestId := ReqId(r.ReqId)
	partialData, err := RetrieveSinglePartialData(requestId, nodeId, cache.Persist)

	if err != nil {
		return false, fmt.Errorf("request %s - simple node %s - %v", r.ReqId, simpleNode.Id, err)
	}
	err = simpleNode.CheckInput(partialData.Data)
	if err != nil {
		return false, err
	}
	// executing node
	output, err := simpleNode.Exec(r, partialData.Data)
	if err != nil {
		return false, err
	}

	// Todo: uncomment when running TestInvokeFC_Concurrent to debug concurrency errors
	// errDbg := Debug(r, string(simpleNode.Id), output)
	// if errDbg != nil {
	// 	return false, errDbg
	// }

	forNode := simpleNode.GetNext()[0]
	pd = NewPartialData(requestId, forNode, nodeId, output)
	errSend := simpleNode.PrepareOutput(dag, output)
	if errSend != nil {
		return false, fmt.Errorf("the node %s cannot send the output: %v", simpleNode.ToString(), errSend)
	}

	// saving partial data and updating progress
	err = SavePartialData(pd, cache.Persist)
	if err != nil {
		return false, err
	}
	err = progress.CompleteNode(nodeId)
	if err != nil {
		return false, err
	}

	return true, nil
}

func (dag *Dag) executeChoice(progress *Progress, choice *ChoiceNode, r *CompositionRequest) (bool, error) {
	// retrieving input
	var pd *PartialData
	nodeId := choice.GetId()
	requestId := ReqId(r.ReqId)
	partialData, err := RetrieveSinglePartialData(requestId, nodeId, cache.Persist)
	if err != nil {
		return false, fmt.Errorf("request %s - choice node %s - %v", r.ReqId, choice.Id, err)
	}
	err = choice.CheckInput(partialData.Data)
	if err != nil {
		return false, err
	}
	// executing node
	output, err := choice.Exec(r, partialData.Data)
	if err != nil {
		return false, err
	}
	pd = NewPartialData(requestId, choice.GetNext()[0], nodeId, output)
	errSend := choice.PrepareOutput(dag, output)
	if errSend != nil {
		return false, fmt.Errorf("the node %s cannot send the output: %v", choice.ToString(), errSend)
	}

	// for choice node, we skip all branch that will not be executed
	nodesToSkip := choice.GetNodesToSkip(dag)
	errSkip := progress.SkipAll(nodesToSkip)
	if errSkip != nil {
		return false, errSkip
	}

	// saving partial data and updating progress
	err = SavePartialData(pd, cache.Persist)
	if err != nil {
		return false, err
	}
	err = progress.CompleteNode(nodeId)
	if err != nil {
		return false, err
	}

	return true, nil
}

func (dag *Dag) executeFanOut(progress *Progress, fanOut *FanOutNode, r *CompositionRequest) (bool, error) {
	// retrieving input
	var pd *PartialData
	nodeId := fanOut.GetId()
	requestId := ReqId(r.ReqId)
	partialData, err := RetrieveSinglePartialData(requestId, nodeId, cache.Persist)
	if err != nil {
		return false, fmt.Errorf("request %s - fanOut node %s - %v", r.ReqId, nodeId, err)
	}
	// err = fanOut.CheckInput(partialData.Data)
	// if err != nil {
	//	return false, err
	//}
	// executing node
	output, err := fanOut.Exec(r, partialData.Data)
	if err != nil {
		return false, err
	}
	// sends output to each next node
	errSend := fanOut.PrepareOutput(dag, output)
	if errSend != nil {
		return false, fmt.Errorf("the node %s cannot send the output: %v", fanOut.ToString(), errSend)
	}

	for i, nextNode := range fanOut.GetNext() {
		//inputForNode := make(map[string]interface{})
		//nextDagNode, found := dag.Find(nextNode)
		//if found && nextDagNode.GetNodeType() == Simple {
		//	// get the node signature, to get parameter name
		//	nextFunction, foundFn := function.GetFunction(nextDagNode.(*SimpleNode).Func)
		//	if !foundFn {
		//		return false, fmt.Errorf("cannot find next function to execute")
		//	}
		//	signatureInputs := nextFunction.Signature.GetInputs()
		//	for j, inputDef := range signatureInputs {
		//		if i == j {
		//			inputForNode[inputDef.Name] = output[fmt.Sprintf("%d", i)]
		//		}
		//	}
		if fanOut.Type == Broadcast {
			pd = NewPartialData(requestId, nextNode, nodeId, output[fmt.Sprintf("%d", i)].(map[string]interface{}))
		} else if fanOut.Type == Scatter {
			firstName := ""
			for name := range output {
				firstName = name
				break
			}
			inputForNode := make(map[string]interface{})
			subMap, found := output[firstName].(map[string]interface{})
			if !found {
				return false, fmt.Errorf("cannot find parameter for nextNode %s", nextNode)
			}
			inputForNode[firstName] = subMap[fmt.Sprintf("%d", i)]
			pd = NewPartialData(requestId, nextNode, nodeId, inputForNode)
		} else {
			return false, fmt.Errorf("invalid fanout type %d", fanOut.Type)
		}
		// saving partial data
		err = SavePartialData(pd, cache.Persist)
		if err != nil {
			return false, err
		}
		//}
	}
	// and updating progress
	err = progress.CompleteNode(nodeId)
	if err != nil {
		return false, err
	}

	return true, nil
}

func (dag *Dag) executeParallel(progress *Progress, nextNodes []DagNodeId, r *CompositionRequest) error {
	// preparing dag nodes and channels for parallel execution
	parallelDagNodes := make([]DagNode, 0)
	inputs := make([]map[string]interface{}, 0)
	outputChannels := make([]chan map[string]interface{}, 0)
	errorChannels := make([]chan error, 0)
	partialDatas := make([]*PartialData, 0)
	requestId := ReqId(r.ReqId)
	for _, nodeId := range nextNodes {
		node, ok := dag.Find(nodeId)
		if ok {
			parallelDagNodes = append(parallelDagNodes, node)
			outputChannels = append(outputChannels, make(chan map[string]interface{}))
			errorChannels = append(errorChannels, make(chan error))
		}
		// for simple node we also retrieve the partial data and receive input
		if simple, isSimple := node.(*SimpleNode); isSimple {
			partialData, err := RetrieveSinglePartialData(requestId, simple.Id, cache.Persist)
			if err != nil {
				return err
			}
			errInput := simple.CheckInput(partialData.Data)
			if errInput != nil {
				return errInput
			}
			inputs = append(inputs, partialData.Data)
		}
	}
	// executing all nodes in parallel
	for i, node := range parallelDagNodes {
		go func(i int, params map[string]interface{}, node DagNode) {
			output, err := node.Exec(r, params)
			// for simple node, we also prepare output
			if simpleNode, isSimple := node.(*SimpleNode); isSimple {
				errSend := simpleNode.PrepareOutput(dag, output)
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
			// fmt.Printf("goroutine %d for node %s completed\n", i, node.GetId())
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
		return fmt.Errorf("errors in parallel execution: %v", parallelErrors)
	}
	// saving partial data
	for i, output := range parallelOutputs {
		node := parallelDagNodes[i]
		pd := NewPartialData(requestId, node.GetNext()[0], node.GetId(), nil)
		partialDatas = append(partialDatas, pd)
		pd.Data = output
		err := SavePartialData(pd, cache.Persist)
		if err != nil {
			return err
		}
		err = progress.CompleteNode(parallelDagNodes[i].GetId())
		if err != nil {
			return err
		}
	}
	return nil
}

func (dag *Dag) executeFanIn(progress *Progress, fanIn *FanInNode, r *CompositionRequest) (bool, error) {
	nodeId := fanIn.GetId()
	requestId := ReqId(r.ReqId)

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

	// retrieving input before timeout
	var partialDatas []*PartialData
	var err error
	for !timerElapsed {
		partialDatas, err = RetrievePartialData(requestId, nodeId, cache.Persist)
		if err != nil {
			return false, err
		}
		if len(partialDatas) == fanIn.FanInDegree {
			break
		}
		fmt.Printf("fanin waiting partial datas: %d/%d\n", len(partialDatas), fanIn.FanInDegree)
		time.Sleep(fanIn.Timeout / 100)
	}

	fired := timer.Stop()
	if !fired {
		return false, fmt.Errorf("fan in timeout occurred")
	}
	faninInputs := make([]map[string]interface{}, 0)
	for _, partialData := range partialDatas {
		// err := fanIn.CheckInput(partialData.Data)
		//if err != nil {
		//	return false, err
		//}
		faninInputs = append(faninInputs, partialData.Data)
	}

	// merging input into one output
	output, err := fanIn.Exec(r, faninInputs...)
	if err != nil {
		return false, err
	}
	// saving merged outputs and updating progress
	pd := NewPartialData(requestId, fanIn.GetNext()[0], nodeId, output)
	err = SavePartialData(pd, cache.Persist)
	if err != nil {
		return false, err
	}
	err = progress.CompleteNode(nodeId)
	if err != nil {
		return false, err
	}

	return true, nil
}

func (dag *Dag) executeEnd(progress *Progress, node *EndNode, r *CompositionRequest) (bool, error) {
	// r.ExecReport.Reports[CreateExecutionReportId(node)] = &function.ExecutionReport{Result: "end"}
	r.ExecReport.Reports.Set(CreateExecutionReportId(node), &function.ExecutionReport{Result: "end"})
	err := progress.CompleteNode(node.Id)
	if err != nil {
		return false, err
	}
	return false, nil // false because we want to stop when reaching the end
}

func (dag *Dag) Execute(r *CompositionRequest) (bool, error) {
	requestId := ReqId(r.ReqId)
	progress, found := RetrieveProgress(requestId, cache.Persist)
	if !found {
		return false, fmt.Errorf("progress not found")
	}
	nextNodes, err := progress.NextNodes()
	if err != nil {
		return false, fmt.Errorf("failed to get next nodes from progress: %v", err)
	}
	shouldContinue := true
	if len(nextNodes) > 1 {
		err := dag.executeParallel(progress, nextNodes, r)
		if err != nil {
			return true, err
		}
	} else {
		n, ok := dag.Find(nextNodes[0])
		if !ok {
			return true, fmt.Errorf("failed to find node %s", n.GetId())
		}

		switch node := n.(type) {
		case *SimpleNode:
			shouldContinue, err = dag.executeSimple(progress, node, r)
		case *ChoiceNode:
			shouldContinue, err = dag.executeChoice(progress, node, r)
		case *FanInNode:
			shouldContinue, err = dag.executeFanIn(progress, node, r)
		case *StartNode:
			shouldContinue, err = dag.executeStart(progress, node, r)
		case *FanOutNode:
			shouldContinue, err = dag.executeFanOut(progress, node, r)
		case *EndNode:
			shouldContinue, err = dag.executeEnd(progress, node, r)
		}
		if err != nil {
			_ = progress.FailNode(n.GetId())
			r.ExecReport.Progress = progress
			return true, err
		}
	}

	err = SaveProgress(progress, cache.Persist)
	if err != nil {
		return true, err
	}
	return shouldContinue, nil
}

// GetUniqueDagFunctions returns a list with the function names used in the Dag. The returned function names are unique and in alphabetical order
func (dag *Dag) GetUniqueDagFunctions() []string {
	allFunctionsMap := make(map[string]void)
	for _, node := range dag.Nodes {
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

func (dag *Dag) Equals(comparer types.Comparable) bool {

	dag2 := comparer.(*Dag)

	for k := range dag.Nodes {
		if !dag.Nodes[k].Equals(dag2.Nodes[k]) {
			return false
		}
	}
	return dag.Start.Equals(dag2.Start) &&
		dag.End.Equals(dag2.End) &&
		dag.Width == dag2.Width &&
		len(dag.Nodes) == len(dag2.Nodes)
}

func (dag *Dag) String() string {
	return fmt.Sprintf(`Dag{
		Start: %s,
		Nodes: %s,
		End:   %s,
		Width: %d,
	}`, dag.Start.ToString(), dag.Nodes, dag.End.ToString(), dag.Width)
}

// MarshalJSON is needed because DagNode is an interface
func (dag *Dag) MarshalJSON() ([]byte, error) {
	// Create a map to hold the JSON representation of the Dag
	data := make(map[string]interface{})

	// Add the field to the map
	data["Start"] = dag.Start
	data["End"] = dag.End
	data["Width"] = dag.Width
	nodes := make(map[DagNodeId]interface{})

	// Marshal the interface and store it as concrete node value in the map
	for nodeId, node := range dag.Nodes {
		switch concreteNode := node.(type) {
		case *StartNode:
			nodes[nodeId] = concreteNode
		case *EndNode:
			nodes[nodeId] = concreteNode
		case *SimpleNode:
			nodes[nodeId] = concreteNode
		case *ChoiceNode:
			nodes[nodeId] = concreteNode
		case *FanOutNode:
			nodes[nodeId] = concreteNode
		case *FanInNode:
			nodes[nodeId] = concreteNode
		default:
			return nil, fmt.Errorf("unsupported Simpatica type")
		}
	}
	data["Nodes"] = nodes

	// Marshal the map to JSON
	return json.Marshal(data)
}

// UnmarshalJSON is needed because DagNode is an interface
func (dag *Dag) UnmarshalJSON(data []byte) error {
	// Create a temporary map to decode the JSON data
	var tempMap map[string]json.RawMessage
	if err := json.Unmarshal(data, &tempMap); err != nil {
		return err
	}
	// extract simple fields
	if rawStart, ok := tempMap["Start"]; ok {
		if err := json.Unmarshal(rawStart, &dag.Start); err != nil {
			return err
		}
	} else {
		return fmt.Errorf("missing 'Start' field in JSON")
	}

	if rawEnd, ok := tempMap["End"]; ok {
		if err := json.Unmarshal(rawEnd, &dag.End); err != nil {
			return err
		}
	} else {
		return fmt.Errorf("missing 'End' field in JSON")
	}

	if rawWidth, ok := tempMap["Width"]; ok {
		if err := json.Unmarshal(rawWidth, &dag.Width); err != nil {
			return err
		}
	} else {
		return fmt.Errorf("missing 'Width' field in JSON")
	}

	// Cycle on each map entry and decode the type
	var tempNodeMap map[string]json.RawMessage
	if err := json.Unmarshal(tempMap["Nodes"], &tempNodeMap); err != nil {
		return err
	}
	dag.Nodes = make(map[DagNodeId]DagNode)
	for nodeId, value := range tempNodeMap {
		err := dag.decodeNode(nodeId, value)
		if err != nil {
			return err
		}
	}
	return nil
}

func (dag *Dag) decodeNode(nodeId string, value json.RawMessage) error {
	var tempNodeMap map[string]interface{}
	if err := json.Unmarshal(value, &tempNodeMap); err != nil {
		return err
	}
	dagNodeType := int(tempNodeMap["NodeType"].(float64))
	var err error
	switch dagNodeType {
	case Start:
		node := &StartNode{}
		err = json.Unmarshal(value, node)
		if err == nil && node.Id != "" && node.Next != "" {
			dag.Nodes[DagNodeId(nodeId)] = node
			return nil
		}
	case End:
		node := &EndNode{}
		err = json.Unmarshal(value, node)
		if err == nil && node.Id != "" {
			dag.Nodes[DagNodeId(nodeId)] = node
			return nil
		}
	case Simple:
		node := &SimpleNode{}
		err = json.Unmarshal(value, node)
		if err == nil && node.Id != "" && node.Func != "" {
			dag.Nodes[DagNodeId(nodeId)] = node
			return nil
		}
	case Choice:
		node := &ChoiceNode{}
		err = json.Unmarshal(value, node)
		if err == nil && node.Id != "" && node.Alternatives != nil && len(node.Alternatives) == len(node.Conditions) {
			dag.Nodes[DagNodeId(nodeId)] = node
			return nil
		}
	case FanOut:
		node := &FanOutNode{}
		err = json.Unmarshal(value, node)
		if err == nil && node.Id != "" {
			dag.Nodes[DagNodeId(nodeId)] = node
			return nil
		}
	case FanIn:
		node := &FanInNode{}
		err = json.Unmarshal(value, node)
		if err == nil && node.Id != "" {
			dag.Nodes[DagNodeId(nodeId)] = node
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

// Debug can be used to find if expected output of test TestInvokeFC_Concurrent is correct based on requestId of format "goroutine_#" and simple node ids of format "simple #"
func Debug(r *CompositionRequest, nodeId string, output map[string]interface{}) error {
	if strings.Contains(r.ReqId, "goroutine") {
		// getting number of goroutine, to get the starting input of the sequence
		startingInput := strings.Split(r.ReqId, "_")[1]
		atoi, errAtoi1 := strconv.Atoi(startingInput)
		if errAtoi1 != nil {
			return errAtoi1
		}
		// getting the number of simple node, to get the increment value up to this node in the sequence
		currentIncrementMinusOne := strings.Split(nodeId, " ")[1]
		atoiInc, errAtoi2 := strconv.Atoi(currentIncrementMinusOne)
		if errAtoi2 != nil {
			return errAtoi2
		}
		expectedOutput := atoi + atoiInc + 1

		// getting the key of the single map entry
		key := ""
		for name := range output {
			key = name
			break
		}
		// assertEquals
		actualOutput := output[key]
		if expectedOutput != actualOutput {
			contents := GetCacheContents()
			fmt.Println(contents)
			return fmt.Errorf("expected: %d - actual: %v\n", expectedOutput, output)
		}
	}
	return nil
}
