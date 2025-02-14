package workflow

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/serverledge-faas/serverledge/utils"
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
	ReqId          ReqId // requestId, used to distinguish different workflow's progresses
	Status         map[TaskId]TaskStatus
	ReadyToExecute []TaskId
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
	Start   TaskType = "StartTask"
	End     TaskType = "EndTask"
	Simple  TaskType = "SimpleTask"
	Choice  TaskType = "ChoiceTask"
	FanOut  TaskType = "FanOutTask"
	FanIn   TaskType = "FanInTask"
	Fail    TaskType = "Fail"
	Succeed TaskType = "SuccessTask"
	Pass    TaskType = "PassTask"
	Wait    TaskType = "WaitNode"
)

func TaskFromType(nodeType TaskType) Task {
	switch nodeType {
	case Start:
		return &StartTask{}
	case End:
		return &EndTask{}
	case Simple:
		return &SimpleTask{}
	case Choice:
		return &ChoiceTask{}
	case FanOut:
		return &FanOutTask{}
	case FanIn:
		return &FanInTask{}
	case Fail:
		return &FailureTask{}
	case Succeed:
		return &SuccessTask{}
	case Pass:
		return &PassTask{}
	default:
		return &SimpleTask{}
	}
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

func (p *Progress) PopNextReady() (TaskId, error) {
	if len(p.ReadyToExecute) == 0 {
		return "", errors.New("no ready tasks")
	}

	t := p.ReadyToExecute[0]
	p.ReadyToExecute = p.ReadyToExecute[1:]
	return t, nil
}

func (p *Progress) PopAllNextReady() ([]TaskId, error) {
	if len(p.ReadyToExecute) == 0 {
		return nil, errors.New("no ready tasks")
	}

	t := p.ReadyToExecute
	p.ReadyToExecute = make([]TaskId, 0)
	return t, nil
}

func (p *Progress) IsCompleted() bool {
	// TODO: might be more efficient checking on ReadyToExecute
	for _, s := range p.Status {
		if s == Pending {
			return false
		}
	}
	return true

}

// Complete sets the progress status of the node with the id input to 'Completed'
func (p *Progress) Complete(id TaskId) {
	p.Status[id] = Executed

	for i, nid := range p.ReadyToExecute {
		if nid == id {
			p.ReadyToExecute = append(p.ReadyToExecute[:i], p.ReadyToExecute[i+1:]...)
			break
		}
	}
}

func (p *Progress) AddReadyTask(id TaskId) error {
	if !p.IsReady(id) {
		return fmt.Errorf("the task is not ready")
	}
	p.ReadyToExecute = append(p.ReadyToExecute, id)
	return nil
}

// TODO: skip on cascade next nodes
func (p *Progress) Skip(id TaskId) {
	p.Status[id] = Skipped

	for i, nid := range p.ReadyToExecute {
		if nid == id {
			p.ReadyToExecute = append(p.ReadyToExecute[:i], p.ReadyToExecute[i+1:]...)
			break
		}
	}
}

// TODO: skip on cascade next nodes
// FailureTask marks a node progress to failed
func (p *Progress) Fail(id TaskId) {
	p.Status[id] = Failed

	for i, nid := range p.ReadyToExecute {
		if nid == id {
			p.ReadyToExecute = append(p.ReadyToExecute[:i], p.ReadyToExecute[i+1:]...)
			break
		}
	}
}

// InitProgress initialize the progress
func InitProgress(reqId ReqId, workflow *Workflow) *Progress {
	statusMap := make(map[TaskId]TaskStatus)
	for _, node := range workflow.Tasks {
		statusMap[node.GetId()] = Pending
	}

	p := &Progress{
		ReqId:          reqId,
		Status:         statusMap,
		ReadyToExecute: make([]TaskId, 0),
	}

	p.ReadyToExecute = append(p.ReadyToExecute, workflow.Start.GetId())

	return p
}

func (p *Progress) Print() {
	fmt.Printf("%s", p.PrettyString())
}

func (p *Progress) PrettyString() string {
	str := fmt.Sprintf("\nProgress for workflow request %s - G = node group, B = node branch\n", p.ReqId)
	str += fmt.Sprintln("(        TaskID        ) - Status")
	str += fmt.Sprintln("-------------------------------------------------")
	for id, s := range p.Status {
		str += fmt.Sprintf("(%-22s) - %s\n", id, printStatus(s))
	}
	return str
}

func (p *Progress) String() string {
	tasks := "["
	for i, s := range p.Status {
		tasks += fmt.Sprintf("(%-22s: %s) ", i, printStatus(s))
		tasks += " "
	}
	tasks += "]"

	return tasks
}

func (p *Progress) Equals(p2 *Progress) bool {

	if len(p.Status) != len(p2.Status) {
		return false
	}

	for i, s := range p.Status {
		s2, ok := p2.Status[i]
		if !ok || s != s2 {
			return false
		}
	}

	return p.ReqId == p2.ReqId
}

func (p *Progress) IsReady(id TaskId) bool {
	// TODO: check also that all previous tasks are complete!
	return p.Status[id] == Pending
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
