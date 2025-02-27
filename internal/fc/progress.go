package fc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/cornelk/hashmap"
	"github.com/grussorusso/serverledge/utils"
)

type ProgressId string

var progressMutexCache = &sync.Mutex{}
var progressMutexEtcd = &sync.Mutex{}
var progressCache = &sync.Map{} // Map[ProgressId, *Progress]

func newProgressId(reqId ReqId) ProgressId {
	return ProgressId("progress_" + reqId)
}

// Progress tracks the progress of a Workflow, i.e. which nodes are executed, and what is the next node to run. Workflow progress is saved in ETCD and retrieved by the next node
type Progress struct {
	ReqId     ReqId // requestId, used to distinguish different workflow's progresses
	TaskInfos []*TaskInfo
	NextGroup int
}

type ProgressCache struct {
	progresses hashmap.Map[ProgressId, *Progress] // a lock-free thread-safe map
}

type TaskInfo struct {
	Id     TaskId
	Type   TaskType
	Status TaskStatus
	Group  int // The group helps represent the order of execution of nodes. Nodes with the same group should run concurrently
	Branch int // copied from task
}

func newNodeInfo(dNode Task, group int) *TaskInfo {
	return &TaskInfo{
		Id:     dNode.GetId(),
		Type:   parseType(dNode),
		Status: Pending,
		Group:  group,
		Branch: dNode.GetBranchId(),
	}
}

func (ni *TaskInfo) Equals(ni2 *TaskInfo) bool {
	return ni.Id == ni2.Id && ni.Type == ni2.Type && ni.Status == ni2.Status && ni.Group == ni2.Group && ni.Branch == ni2.Branch
}

type TaskStatus string

const (
	Pending  = "Pending"
	Executed = "Executed"
	Skipped  = "Skipped" // if a node is skipped, all its children nodes should also be skipped
	Failed   = "Failed"
)

func printStatus(s TaskStatus) string {
	switch s {
	case Pending:
		return "Pending"
	case Executed:
		return "Executed"
	case Skipped:
		return "Skipped"
	case Failed:
		return "Failed"
	}
	return "No Status - Error"
}

type TaskType string

const (
	Start   TaskType = "StartNode"
	End     TaskType = "EndNode"
	Simple  TaskType = "SimpleNode"
	Choice  TaskType = "ChoiceNode"
	FanOut  TaskType = "FanOutNode"
	FanIn   TaskType = "FanInNode"
	Fail    TaskType = "FailNode"
	Succeed TaskType = "SucceedNode"
	Pass    TaskType = "PassNode"
	Wait    TaskType = "WaitNode"
)

func TaskFromType(nodeType TaskType) Task {
	switch nodeType {
	case Start:
		return &StartNode{}
	case End:
		return &EndNode{}
	case Simple:
		return &SimpleNode{}
	case Choice:
		return &ChoiceNode{}
	case FanOut:
		return &FanOutNode{}
	case FanIn:
		return &FanInNode{}
	case Fail:
		return &FailNode{}
	case Succeed:
		return &SucceedNode{}
	case Pass:
		return &PassNode{}
	case Wait:
		return &WaitNode{}
	default:
		return &SimpleNode{}
	}
}

func parseType(dNode Task) TaskType {
	switch dNode.(type) {
	case *StartNode:
		return Start
	case *EndNode:
		return End
	case *SimpleNode:
		return Simple
	case *ChoiceNode:
		return Choice
	case *FanOutNode:
		return FanOut
	case *FanInNode:
		return FanIn
	case *FailNode:
		return Fail
	case *SucceedNode:
		return Succeed
	case *PassNode:
		return Pass
	case *WaitNode:
		return Wait
	}

	panic("unreachable!")
}
func printType(t TaskType) string {
	switch t {
	case Start:
		return "Start"
	case End:
		return "End"
	case Simple:
		return "Simple"
	case Choice:
		return "Choice"
	case FanOut:
		return "FanOut"
	case FanIn:
		return "FanIn"
	case Fail:
		return "Fail"
	case Succeed:
		return "Succeed"
	case Pass:
		return "Pass"
	case Wait:
		return "Wait"
	}
	return ""
}

func (p *Progress) IsCompleted() bool {
	for _, node := range p.TaskInfos {
		if node.Status == Pending {
			return false
		}
	}
	return true

}

// NextNodes retrieves the next nodes to execute, that have the minimum group with state pending
func (p *Progress) NextNodes() ([]TaskId, error) {
	minPendingGroup := -1
	// find the min group with node pending
	for _, node := range p.TaskInfos {
		if node.Status == Pending {
			minPendingGroup = node.Group
			break
		}
		if node.Status == Failed {
			return []TaskId{}, fmt.Errorf("the execution is failed ")
		}
	}
	// get all node Ids within that group
	nodeIds := make([]TaskId, 0)
	for _, node := range p.TaskInfos {
		if node.Group == minPendingGroup && node.Status == Pending {
			nodeIds = append(nodeIds, node.Id)
		}
	}
	p.NextGroup = minPendingGroup
	return nodeIds, nil
}

// CompleteNode sets the progress status of the node with the id input to 'Completed'
func (p *Progress) CompleteNode(id TaskId) error {
	for _, node := range p.TaskInfos {
		if node.Id == id {
			node.Status = Executed
			return nil
		}
	}
	return fmt.Errorf("no node to complete with id %s exists in the workflow for request %s", id, p.ReqId)
}

func (p *Progress) SkipNode(id TaskId) error {
	for _, node := range p.TaskInfos {
		if node.Id == id {
			node.Status = Skipped
			// fmt.Printf("skipped node %s\n", id)
			return nil
		}
	}
	return fmt.Errorf("no node to skip with id %s exists in the workflow for request %s", id, p.ReqId)
}

func (p *Progress) SkipAll(nodes []Task) error {
	for _, node := range nodes {
		err := p.SkipNode(node.GetId())
		if err != nil {
			return err
		}
	}
	return nil
}

// FailNode marks a node progress to failed
func (p *Progress) FailNode(id TaskId) error {
	for _, node := range p.TaskInfos {
		if node.Id == id {
			node.Status = Failed
			return nil
		}
	}
	return fmt.Errorf("no node to fail with id %s exists in the workflow for request %s", id, p.ReqId)
}

func (p *Progress) GetInfo(nodeId TaskId) *TaskInfo {
	for _, node := range p.TaskInfos {
		if node.Id == nodeId {
			return node
		}
	}
	return nil
}

func (p *Progress) GetGroup(nodeId TaskId) int {
	for _, node := range p.TaskInfos {
		if node.Id == nodeId {
			return node.Group
		}
	}
	return -1
}

// moveEndNodeAtTheEnd moves the end node at the end of the list and sets its group accordingly
func moveEndNodeAtTheEnd(nodeInfos []*TaskInfo) []*TaskInfo {
	// move the endNode at the end of the list
	var endNodeInfo *TaskInfo
	// get index of end node to remove
	indexToRemove := -1
	maxGroup := 0
	for i, nodeInfo := range nodeInfos {
		if nodeInfo.Type == End {
			indexToRemove = i
			endNodeInfo = nodeInfo
			continue
		}
		if nodeInfo.Group > maxGroup {
			maxGroup = nodeInfo.Group
		}
	}
	if indexToRemove != -1 {
		// remove end node
		nodeInfos = append(nodeInfos[:indexToRemove], nodeInfos[indexToRemove+1:]...)
		// update endNode group
		endNodeInfo.Group = maxGroup + 1
		// append at the end of the visited node list
		nodeInfos = append(nodeInfos, endNodeInfo)
	}
	return nodeInfos
}

// InitProgressRecursive initialize the node list assigning a group to each node, so that we can know which nodes should run in parallel or is a choice branch
func InitProgressRecursive(reqId ReqId, workflow *Workflow) *Progress {
	nodeInfos := extractNodeInfo(workflow, workflow.Start, 0, make([]*TaskInfo, 0))
	nodeInfos = moveEndNodeAtTheEnd(nodeInfos)
	nodeInfos = reorder(nodeInfos)
	return &Progress{
		ReqId:     reqId,
		TaskInfos: nodeInfos,
		NextGroup: 0,
	}
}

// popMinGroupAndBranchNode removes the node with minimum group and, in case of multiple nodes in the same group, minimum branch
func popMinGroupAndBranchNode(infos *[]*TaskInfo) *TaskInfo {
	// finding min group nodes
	minGroup := math.MaxInt
	var minGroupNodeInfo []*TaskInfo
	for _, info := range *infos {
		if info.Group < minGroup {
			minGroupNodeInfo = make([]*TaskInfo, 0)
			minGroup = info.Group
			minGroupNodeInfo = append(minGroupNodeInfo, info)
		}
		if info.Group == minGroup {
			minGroupNodeInfo = append(minGroupNodeInfo, info)
		}
	}
	minBranch := math.MaxInt // when there are ties
	var minGroupAndBranchNode *TaskInfo

	// finding min branch node from those of the minimum group
	for _, info := range minGroupNodeInfo {
		if info.Branch < minBranch {
			minBranch = info.Branch
			minGroupAndBranchNode = info
		}
	}

	// finding index to remove from starting list
	var indexToRemove int
	for i, info := range *infos {
		if info.Id == minGroupAndBranchNode.Id {
			indexToRemove = i
			break
		}
	}
	*infos = append((*infos)[:indexToRemove], (*infos)[indexToRemove+1:]...)
	return minGroupAndBranchNode
}

func reorder(infos []*TaskInfo) []*TaskInfo {
	reordered := make([]*TaskInfo, 0)
	for len(infos) > 0 {
		next := popMinGroupAndBranchNode(&infos)
		reordered = append(reordered, next)
	}
	return reordered
}

func isNodeInfoPresent(node TaskId, infos []*TaskInfo) bool {
	isPresent := false
	for _, nodeInfo := range infos {
		if nodeInfo.Id == node {
			isPresent = true
			break
		}
	}
	return isPresent
}

// extractNodeInfo retrieves all needed information from nodes and sets node groups. It duplicates end nodes.
func extractNodeInfo(workflow *Workflow, node Task, group int, infos []*TaskInfo) []*TaskInfo {
	info := newNodeInfo(node, group)
	if !isNodeInfoPresent(node.GetId(), infos) {
		infos = append(infos, info)
	} else if n, ok := node.(*FanInNode); ok {
		for _, nodeInfo := range infos {
			if nodeInfo.Id == n.GetId() {
				nodeInfo.Group = group
				break
			}
		}
	}
	group++
	switch n := node.(type) {
	case *StartNode:
		startNode, _ := workflow.Find(n.GetNext()[0])
		toAdd := extractNodeInfo(workflow, startNode, group, infos)
		for _, add := range toAdd {
			if !isNodeInfoPresent(add.Id, infos) {
				infos = append(infos, add)
			}
		}
		return infos
	case *SimpleNode, *PassNode, *WaitNode, *SucceedNode, *FailNode:
		task, _ := workflow.Find(n.GetNext()[0])
		toAdd := extractNodeInfo(workflow, task, group, infos)
		for _, add := range toAdd {
			if !isNodeInfoPresent(add.Id, infos) {
				infos = append(infos, add)
			}
		}
		return infos
	case *EndNode:
		return infos
	case *ChoiceNode:
		for _, alternativeId := range n.Alternatives {
			alternative, _ := workflow.Find(alternativeId)
			toAdd := extractNodeInfo(workflow, alternative, group, infos)
			for _, add := range toAdd {
				if !isNodeInfoPresent(add.Id, infos) {
					infos = append(infos, add)
				}
			}
		}
		return infos
	case *FanOutNode:
		for _, parallelBranchId := range n.GetNext() {
			parallelBranch, _ := workflow.Find(parallelBranchId)
			toAdd := extractNodeInfo(workflow, parallelBranch, group, infos)
			for _, add := range toAdd {
				if !isNodeInfoPresent(add.Id, infos) {
					infos = append(infos, add)
				}
			}
		}
		return infos
	case *FanInNode:
		fanInNode, _ := workflow.Find(n.GetNext()[0])
		toAdd := extractNodeInfo(workflow, fanInNode, group, infos)
		for _, add := range toAdd {
			if !isNodeInfoPresent(add.Id, infos) {
				infos = append(infos, add)
			}
		}
	}
	return infos
}

func (p *Progress) Print() {
	fmt.Printf("%s", p.PrettyString())
}

func (p *Progress) PrettyString() string {
	str := fmt.Sprintf("\nProgress for composition request %s - G = node group, B = node branch\n", p.ReqId)
	str += fmt.Sprintln("G. |B| Type   (        NodeID        ) - Status")
	str += fmt.Sprintln("-------------------------------------------------")
	for _, info := range p.TaskInfos {
		str += fmt.Sprintf("%d. |%d| %-6s (%-22s) - %s\n", info.Group, info.Branch, printType(info.Type), info.Id, printStatus(info.Status))
	}
	return str
}

func (p *Progress) String() string {
	tasks := "["
	for i, node := range p.TaskInfos {
		tasks += string(node.Id)
		if i != len(p.TaskInfos)-1 {
			tasks += ", "
		}
	}
	tasks += "]"

	return fmt.Sprintf(`Progress{
		ReqId:     %s,
		TaskInfos:  %s,
		NextGroup: %d,
	}`, p.ReqId, tasks, p.NextGroup)
}

func (p *Progress) Equals(p2 *Progress) bool {
	for i := range p.TaskInfos {
		if !p.TaskInfos[i].Equals(p2.TaskInfos[i]) {
			return false
		}
	}

	return p.ReqId == p2.ReqId && p.NextGroup == p2.NextGroup
}

// SaveProgress should be used by a completed node after its execution
func SaveProgress(p *Progress, alsoOnEtcd bool) error {
	// save progress to etcd and in cache
	if alsoOnEtcd {
		err := saveProgressToEtcd(p)
		if err != nil {
			return err
		}
	}
	inCache := saveProgressInCache(p)
	if !inCache {
		return errors.New("failed to save progress in cache")
	}
	return nil
}

// RetrieveProgress should be used by the next node to execute
func RetrieveProgress(reqId ReqId, tryFromEtcd bool) (*Progress, bool) {
	var err error

	// Get from cache if exists, otherwise from ETCD
	progress, found := getProgressFromCache(newProgressId(reqId))
	if !found && tryFromEtcd {
		// cache miss - retrieve progress from ETCD
		progress, err = getProgressFromEtcd(reqId)
		if err != nil {
			return nil, false
		}
		// insert a new element to the cache
		ok := saveProgressInCache(progress)
		if !ok {
			return nil, false
		}
		return progress, true
	}

	return progress, found
}

func DeleteProgress(reqId ReqId, alsoFromEtcd bool) error {
	// Remove the progress from the local cache
	progressMutexCache.Lock()
	progressCache.Delete(newProgressId(reqId))
	progressMutexCache.Unlock()

	if alsoFromEtcd {
		cli, err := utils.GetEtcdClient()
		if err != nil {
			return fmt.Errorf("failed to connect to etcd: %v", err)
		}
		ctx := context.TODO()
		progressMutexEtcd.Lock()
		defer progressMutexEtcd.Unlock()
		// remove the progress from ETCD
		dresp, err := cli.Delete(ctx, getProgressEtcdKey(reqId))
		if err != nil || dresp.Deleted != 1 {
			return fmt.Errorf("failed progress delete: %v", err)
		}
	}
	return nil
}

func getProgressEtcdKey(reqId ReqId) string {
	return fmt.Sprintf("/progress/%s", reqId)
}

func saveProgressInCache(p *Progress) bool {
	progressIdType := newProgressId(p.ReqId)
	progressMutexCache.Lock()
	defer progressMutexCache.Unlock()
	_, _ = progressCache.LoadOrStore(progressIdType, p) // [ProgressId, *Progress]
	return true
}

func saveProgressToEtcd(p *Progress) error {
	// save in ETCD
	cli, err := utils.GetEtcdClient()
	if err != nil {
		return err
	}
	ctx := context.TODO()
	// marshal the progress object into json
	payload, err := json.Marshal(p)
	if err != nil {
		return fmt.Errorf("could not marshal progress: %v", err)
	}
	// saves the json object into etcd
	key := getProgressEtcdKey(p.ReqId)
	progressMutexEtcd.Lock()
	defer progressMutexEtcd.Unlock()
	_, err = cli.Put(ctx, key, string(payload))
	if err != nil {
		return fmt.Errorf("failed etcd Put: %v", err)
	}
	return nil
}

func getProgressFromCache(progressId ProgressId) (*Progress, bool) {
	c := progressCache
	progressMutexCache.Lock()
	defer progressMutexCache.Unlock()
	progress, found := c.Load(progressId)
	if !found {
		return nil, false
	}
	return progress.(*Progress), true
}

func getProgressFromEtcd(requestId ReqId) (*Progress, error) {
	cli, err := utils.GetEtcdClient()
	if err != nil {
		return nil, errors.New("failed to connect to ETCD")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	key := getProgressEtcdKey(requestId)
	progressMutexEtcd.Lock()
	getResponse, err := cli.Get(ctx, key)
	if err != nil || len(getResponse.Kvs) < 1 {
		progressMutexEtcd.Unlock()
		return nil, fmt.Errorf("failed to retrieve progress for requestId: %s", key)
	}
	progressMutexEtcd.Unlock()

	var progress Progress
	err = json.Unmarshal(getResponse.Kvs[0].Value, &progress)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal progress json: %v", err)
	}

	return &progress, nil
}
