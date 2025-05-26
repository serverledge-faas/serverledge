package workflow

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/serverledge-faas/serverledge/utils"
)

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
	Start    TaskType = "StartTask"
	End      TaskType = "EndTask"
	Function TaskType = "FunctionTask"
	Choice   TaskType = "ChoiceTask"
	Fail     TaskType = "Fail"
	Succeed  TaskType = "SuccessTask"
	Pass     TaskType = "PassTask"
	Wait     TaskType = "WaitNode"
)

func TaskFromType(nodeType TaskType) Task {
	switch nodeType {
	case Start:
		return &StartTask{}
	case End:
		return &EndTask{}
	case Function:
		return &FunctionTask{}
	case Choice:
		return &ChoiceTask{}
	case Fail:
		return &FailureTask{}
	case Succeed:
		return &SuccessTask{}
	case Pass:
		return &PassTask{}
	default:
		return &FunctionTask{}
	}
}

func printType(t TaskType) string {
	switch t {
	case Start:
		return "Start"
	case End:
		return "End"
	case Function:
		return "Function"
	case Choice:
		return "Choice"
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

// Complete sets the progress status of the node with the id input to 'Completed'
func (p *Progress) Complete(id TaskId) {
	p.Status[id] = Executed

	for i, nid := range p.ReadyToExecute {
		if nid == id {
			// pop from the ready queue
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

	p.ReadyToExecute = append(p.ReadyToExecute, workflow.Start.Id)

	return p
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

// SaveProgress saves Progress in Etcd
func SaveProgress(p *Progress) error {
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
	log.Printf("Saving progress with key: %s", key)

	_, err = cli.Put(ctx, key, string(payload))
	if err != nil {
		return fmt.Errorf("failed etcd Put: %v", err)
	}
	return nil
}

// RetrieveProgress retrieves Progress from Etcd
func RetrieveProgress(reqId ReqId) (*Progress, error) {
	cli, err := utils.GetEtcdClient()
	if err != nil {
		return nil, errors.New("failed to connect to ETCD")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	key := getProgressEtcdKey(reqId)
	getResponse, err := cli.Get(ctx, key)
	if err != nil || len(getResponse.Kvs) < 1 {
		return nil, fmt.Errorf("failed to retrieve progress for requestId: %s", key)
	}

	var progress Progress
	err = json.Unmarshal(getResponse.Kvs[0].Value, &progress)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal progress json: %v", err)
	}

	return &progress, nil
}

func DeleteProgress(reqId ReqId) error {
	cli, err := utils.GetEtcdClient()
	if err != nil {
		return fmt.Errorf("failed to connect to etcd: %v", err)
	}
	ctx := context.TODO()
	dresp, err := cli.Delete(ctx, getProgressEtcdKey(reqId))
	if err != nil || dresp.Deleted != 1 {
		return fmt.Errorf("failed progress delete: %v", err)
	}
	return nil
}

func getProgressEtcdKey(reqId ReqId) string {
	return fmt.Sprintf("/progress/%s", reqId)
}
