package test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"runtime"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/serverledge-faas/serverledge/internal/node"
	"github.com/serverledge-faas/serverledge/internal/registration"
	"github.com/serverledge-faas/serverledge/internal/workflow"
	"github.com/serverledge-faas/serverledge/utils"
	"github.com/stretchr/testify/assert"
	"golang.org/x/exp/slices"
)

// testOffloadingPolicy uses the real decision logic but overrides the remote host URL for testing.
type testOffloadingPolicy struct {
	placementPlan workflow.TaskPlacement
	nodeURLs      map[string]string // Map Node NAME -> URL (e.g., "B" -> "http://127.0.0.1:...")
}

func (p *testOffloadingPolicy) Init() {}

func (p *testOffloadingPolicy) Evaluate(r *workflow.Request, progress *workflow.Progress, runningTasks map[workflow.TaskId]bool) ([]workflow.OffloadingDecision, error) {
	if !r.CanDoOffloading {
		return []workflow.OffloadingDecision{{Offload: false}}, nil
	}

	return workflow.ComputeDecisionFromPlacement(p.placementPlan, progress, r), nil
}

// mockNode represents a fake remote Serverledge node that correctly updates workflow progress
type mockNode struct {
	server      *httptest.Server
	invocations []string
	mutex       sync.Mutex
}

// newMockNode creates and starts a new mock node
func newMockNode(t *testing.T, name string, wf *workflow.Workflow) *mockNode {
	mockServer := &mockNode{}
	handler := http.HandlerFunc(func(w http.ResponseWriter, reqHTTP *http.Request) {
		arrival := time.Now()

		body, _ := io.ReadAll(reqHTTP.Body)
		var req workflow.WorkflowInvocationResumeRequest
		if json.Unmarshal(body, &req) != nil {
			http.Error(w, "invalid request body", http.StatusBadRequest)
			return
		}

		resumeReq := &workflow.Request{
			Id:              req.ReqId,
			W:               wf,
			CanDoOffloading: false,
			Plan:            &req.Plan,
			ExecReport:      workflow.ExecutionReport{},
			Resuming:        true,
			Progress:        req.Progress,
			InitialData:     req.Data,
		}

		err := wf.Invoke(resumeReq)
		if err != nil {
			log.Printf("[%s] Errore in Invoke: %v", name, err)
			http.Error(w, "Invoke failed", http.StatusInternalServerError)
			return
		}

		mockServer.mutex.Lock()

		mockServer.invocations = append(mockServer.invocations, fmt.Sprintf("inv_on_%s", name))

		mockServer.mutex.Unlock()

		if errors.Is(err, node.OutOfResourcesErr) {
			log.Printf("[Rq-%v] Returning 429 (Out of Resources)", req.ReqId)
			http.Error(w, "", http.StatusTooManyRequests)
			return
		} else if err != nil {
			log.Printf("[Rq-%v] Invocation failed: %v", req.ReqId, err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		log.Printf("[Rq-%v] Invocation succeeded on mockServer %s", req.ReqId, name)

		resumeReq.ExecReport.ResponseTime = time.Since(arrival).Seconds()

		resp := workflow.InvocationResponse{
			Success:        true,
			Result:         resumeReq.ExecReport.Result,
			Reports:        resumeReq.ExecReport.Reports,
			ResponseTime:   resumeReq.ExecReport.ResponseTime,
			SchedulingTime: resumeReq.ExecReport.SchedulingTime,
			ResumeData:     &workflow.ResumeResponseData{ResultingProgress: resumeReq.Progress, NextTasksNotEligible: resumeReq.NextTasksNotEligible, OutputData: resumeReq.OutputData},
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(resp)
	})

	mockServer.server = httptest.NewServer(handler)
	t.Cleanup(mockServer.server.Close)
	return mockServer
}

func TestMultiNodeOffloadingWorkflow(t *testing.T) {
	wfb1, _ := workflow.NewBuilder().
		AddPassNodeWithId("", "b1_t1").
		AddPassNodeWithId("", "b1_t2").
		AddPassNodeWithId("", "b1_t3").
		AddPassNodeWithId("", "b1_t4").
		Build()

	wfb2, _ := workflow.NewBuilder().AddPassNodeWithId("", "b2_t1").AddPassNodeWithId("", "b2_t2").AddPassNodeWithId("", "b2_t3").Build()

	wfb3, _ := workflow.NewBuilder().
		AddPassNodeWithId("", "b3_t1").
		AddPassNodeWithId("", "b3_t2").
		Build()

	wfb5, _ := workflow.NewBuilder().AddPassNodeWithId("", "b5_t1").AddPassNodeWithId("", "b5_t2").Build()

	wfb6, _ := workflow.NewBuilder().AddPassNodeWithId("", "b6_t1").AddPassNodeWithId("", "b6_t2").Build()

	wfb4, _ := workflow.NewBuilder().AddPassNodeWithId("", "b4_t1").AddParallelNode([]*workflow.Workflow{wfb5, wfb6}, "b4_t2").AddPassNodeWithId("", "b4_t3").Build()

	innerCond1 := workflow.NewEqParamCondition(workflow.NewParam("n"), workflow.NewValue(0.0))
	innerCond2 := workflow.NewDiffParamCondition(workflow.NewParam("n"), workflow.NewValue(0.0))

	innerBranch1Func := func() (*workflow.Workflow, error) {
		return workflow.NewBuilder().AddPassNodeWithId("", "nested_branch_1").Build()
	}
	innerBranch2Func := func() (*workflow.Workflow, error) {
		return workflow.NewBuilder().AddPassNodeWithId("", "nested_branch_2").Build()
	}

	wfb_nested_choice, err := workflow.NewBuilder().
		AddPassNodeWithId("", "pre_nested_choice").
		AddChoiceNode(innerCond1, innerCond2).
		NextBranch(innerBranch1Func()).
		NextBranch(innerBranch2Func()).
		EndChoiceAndBuild()
	assert.NoError(t, err)

	var nestedChoiceNodeId workflow.TaskId
	preNestedChoiceNode, _ := wfb_nested_choice.Find(workflow.TaskId("pre_nested_choice"))
	nestedChoiceNodeId = preNestedChoiceNode.(workflow.UnaryTask).GetNext()

	branch1Func := func() (*workflow.Workflow, error) {
		return workflow.NewBuilder().AddPassNodeWithId("", "choice_branch_1").Build()
	}
	branch2Func := func() (*workflow.Workflow, error) {
		return wfb_nested_choice, nil
	}

	cond1 := workflow.NewDiffParamCondition(workflow.NewParam("n"), workflow.NewValue(0.0))
	cond2 := workflow.NewEqParamCondition(workflow.NewParam("n"), workflow.NewValue(0.0))

	wfb_choice, err := workflow.NewBuilder().
		AddPassNodeWithId("", "pre_choice").
		AddChoiceNode(cond1, cond2).
		NextBranch(branch1Func()).
		NextBranch(branch2Func()).
		EndChoiceAndBuild()
	assert.NoError(t, err)

	var choiceNodeId workflow.TaskId
	preChoiceNode, _ := wfb_choice.Find(workflow.TaskId("pre_choice"))
	choiceNodeId = preChoiceNode.(workflow.UnaryTask).GetNext()

	// Main workflow
	wf, err := workflow.NewBuilder().
		AddParallelNode([]*workflow.Workflow{wfb1, wfb2, wfb3, wfb4, wfb_choice}, "parallel").
		AddPassNodeWithId("", "pre_final_task").
		AddPassNodeWithId("", "final_task").
		Build()
	assert.NoError(t, err)
	wf.Name = "offloadingTestWf"

	localNodeKey := registration.SelfRegistration.Key
	placementPlan := make(workflow.TaskPlacement)

	for taskId := range wf.Tasks {
		placementPlan[taskId] = localNodeKey
	}

	placementPlan[workflow.TaskId("b1_t3")] = "B"
	placementPlan[workflow.TaskId("b1_t4")] = "B"
	placementPlan[workflow.TaskId("parallel")] = "B"
	placementPlan[workflow.TaskId("b2_t1")] = "B"
	placementPlan[workflow.TaskId("b4_t1")] = "C"
	placementPlan[workflow.TaskId("b5_t2")] = "C"
	placementPlan[workflow.TaskId("b6_t1")] = "C"
	placementPlan[workflow.TaskId("b6_t2")] = "C"
	placementPlan[choiceNodeId] = "C"
	placementPlan[workflow.TaskId("choice_branch_1")] = "B"
	placementPlan[workflow.TaskId("pre_nested_choice")] = "C"
	placementPlan[nestedChoiceNodeId] = "B"
	placementPlan[workflow.TaskId("nested_branch_1")] = "C"

	// Save the workflow to etcd so mock nodes can find it
	err = wf.Save()
	assert.NoError(t, err)
	t.Cleanup(func() {
		wf.Delete()
	})

	// Setup mock environment
	nodeB := newMockNode(t, "B", wf)
	nodeC := newMockNode(t, "C", wf)

	bURL, _ := url.Parse(nodeB.server.URL)
	bIP, bPortStr, _ := net.SplitHostPort(bURL.Host)
	bPort, _ := strconv.Atoi(bPortStr)

	cURL, _ := url.Parse(nodeC.server.URL)
	cIP, cPortStr, _ := net.SplitHostPort(cURL.Host)
	cPort, _ := strconv.Atoi(cPortStr)

	cli, err := utils.GetEtcdClient()
	assert.NoError(t, err)

	area := registration.SelfRegistration.Area
	arch := runtime.GOARCH

	// Node B
	keyB := fmt.Sprintf("registry/%s/%s/B", area, arch)
	payloadB := fmt.Sprintf("%s;%d;%d;%s", bIP, bPort, 9876, arch)
	_, err = cli.Put(context.Background(), keyB, payloadB)
	assert.NoError(t, err)

	// Node C
	keyC := fmt.Sprintf("registry/%s/%s/C", area, arch)
	payloadC := fmt.Sprintf("%s;%d;%d;%s", cIP, cPort, 9876, arch)
	_, err = cli.Put(context.Background(), keyC, payloadC)
	assert.NoError(t, err)

	log.Println("Attesa del sync di Etcd (discovery dei mock node)...")
	time.Sleep(35 * time.Second)

	// Setup and inject the custom offloading policy
	policy := &testOffloadingPolicy{
		placementPlan: placementPlan,
		nodeURLs: map[string]string{
			"B": nodeB.server.URL,
			"C": nodeC.server.URL,
		},
	}
	workflow.SetOffloadingPolicy(policy)
	t.Cleanup(func() {
		workflow.CreateOffloadingPolicy() // Restore default policy
	})

	// Invoke the workflow
	initialParams := map[string]interface{}{"n": 0}
	request := workflow.NewRequest("test-offload-req", wf, initialParams, 10)
	request.CanDoOffloading = true

	err = wf.Invoke(request)
	assert.NoError(t, err)

	assert.NotNil(t, request.ExecReport.Result, "Workflow should have completed")

	// Check invocations on mock nodes
	assert.Equal(t, 3, len(nodeB.invocations), "Node B should have been invoked 3 times")
	assert.True(t, slices.Contains(nodeB.invocations, "inv_on_B"))

	assert.Equal(t, 5, len(nodeC.invocations), "Node C should have been invoked 5 times")
	assert.True(t, slices.Contains(nodeC.invocations, "inv_on_C"))

	log.Printf("Execution successful. Invocations on B: %v, Invocations on C: %v", nodeB.invocations, nodeC.invocations)
}
