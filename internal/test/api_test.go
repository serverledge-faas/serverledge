package test

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/serverledge-faas/serverledge/internal/function"
	"github.com/serverledge-faas/serverledge/internal/workflow"
	"github.com/serverledge-faas/serverledge/utils"
)

// TestContainerPool executes repeatedly different functions (**not compositions**) to verify the container pool
func TestContainerPool(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test")
	}
	// creating inc and double functions
	funcs := []string{"inc", "double"}
	for _, name := range funcs {
		fn, err := InitializePyFunction(name, "handler", function.NewSignature().
			AddInput("input", function.Int{}).
			AddOutput("result", function.Int{}).
			Build())
		utils.AssertNil(t, err)

		createApiIfNotExistsTest(t, fn, HOST, PORT)
	}
	// executing all functions
	channel := make(chan error)
	const n = 3
	for i := 0; i < n; i++ {
		for _, name := range funcs {
			x := make(map[string]interface{})
			x["input"] = 1
			fnName := name
			go func() {
				time.Sleep(50 * time.Millisecond)
				err := invokeApiTest(fnName, x, HOST, PORT)
				channel <- err
			}()
		}
	}

	// wait for all functions to complete and checking the errors
	for i := 0; i < len(funcs)*n; i++ {
		err := <-channel
		utils.AssertNil(t, err)
	}
	// delete each function
	for _, name := range funcs {
		deleteApiTest(t, name, HOST, PORT)
	}
	//utils.AssertTrueMsg(t, workflow.IsEmptyPartialDataCache(), "partial data cache is not empty")
}

// TestCreateWorkflow tests the compose REST API that creates a new function composition
func TestCreateWorkflow(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test")
	}
	fcName := "sequence"
	oldW, found := workflow.Get(fcName)
	if found {
		oldW.Delete()
	}

	fn, err := InitializePyFunction("inc", "handler", function.NewSignature().
		AddInput("input", function.Int{}).
		AddOutput("result", function.Int{}).
		Build())
	utils.AssertNilMsg(t, err, "failed to initialize function")
	wflow, err := CreateSequenceWorkflow(fn, fn, fn)
	wflow.Name = fcName
	utils.AssertNil(t, err)
	err = createWorkflowApiTest(wflow, HOST, PORT)
	if err != nil {
		fmt.Println(err)
		t.Fail()
	}

	// here we do not use REST API
	getFC, b := workflow.Get(fcName)
	utils.AssertTrue(t, b)
	utils.AssertTrueMsg(t, wflow.Equals(getFC), "composition comparison failed")
	err = wflow.Delete()
	utils.AssertNilMsg(t, err, "failed to delete composition")

}

// TestInvokeWorkflow tests the REST API that executes a given function composition
func TestInvokeWorkflow(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test")
	}
	fcName := "sequence"
	fn, err := initializeJsFunction("inc", function.NewSignature().
		AddInput("input", function.Int{}).
		AddOutput("result", function.Int{}).
		Build())
	utils.AssertNilMsg(t, err, "failed to initialize function")
	wflow, err := CreateSequenceWorkflow(fn, fn, fn)
	wflow.Name = fcName
	utils.AssertNil(t, err)
	err = createWorkflowApiTest(wflow, HOST, PORT)
	if err != nil {
		fmt.Println(err)
		t.Fail()
	}

	// === this is the test ===
	params := make(map[string]interface{})
	params["input"] = 1
	invokeWorkflowApiTest(t, params, fcName, HOST, PORT, false)

	// here we do not use REST API
	getFC, b := workflow.Get(fcName)
	utils.AssertTrue(t, b)
	utils.AssertTrueMsg(t, wflow.Equals(getFC), "composition comparison failed")
	err = wflow.Delete()
	utils.AssertNilMsg(t, err, "failed to delete composition")

}

// TestInvokeWorkflow tests the REST API that executes a given function composition
func TestInvokeWorkflow_DifferentFunctions(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test")
	}
	fcName := "sequence"
	fnJs, err := initializeJsFunction("inc", function.NewSignature().
		AddInput("input", function.Int{}).
		AddOutput("result", function.Int{}).
		Build())
	utils.AssertNilMsg(t, err, "failed to initialize javascript function")
	fnPy, err := InitializePyFunction("double", "handler", function.NewSignature().
		AddInput("input", function.Int{}).
		AddOutput("result", function.Int{}).
		Build())
	utils.AssertNilMsg(t, err, "failed to initialize python function")
	wflow, err := CreateSequenceWorkflow(fnPy, fnJs, fnPy, fnJs)
	wflow.Name = fcName
	utils.AssertNil(t, err)
	err = createWorkflowApiTest(wflow, HOST, PORT)
	if err != nil {
		fmt.Println(err)
		t.Fail()
	}

	// === this is the test ===
	params := make(map[string]interface{})
	params["input"] = 1
	invokeWorkflowApiTest(t, params, fcName, HOST, PORT, false)

	// here we do not use REST API
	getFC, b := workflow.Get(fcName)
	utils.AssertTrue(t, b)
	utils.AssertTrueMsg(t, wflow.Equals(getFC), "composition comparison failed")
	err = wflow.Delete()
	utils.AssertNilMsg(t, err, "failed to delete composition")

}

// TestDeleteWorkflow tests the compose REST API that deletes a function composition
func TestDeleteWorkflow(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test")
	}
	fcName := "sequence"
	fn, err := InitializePyFunction("inc", "handler", function.NewSignature().
		AddInput("input", function.Int{}).
		AddOutput("result", function.Int{}).
		Build())
	if err != nil {
		fmt.Printf("inc creation failed: %v\n", err)
		t.Fail()
	}
	db, err := InitializePyFunction("double", "handler", function.NewSignature().
		AddInput("input", function.Int{}).
		AddOutput("result", function.Int{}).
		Build())
	utils.AssertNilMsg(t, err, "failed to initialize function")
	wflow, err := CreateSequenceWorkflow(fn, db, fn)
	wflow.Name = fcName
	utils.AssertNil(t, err)

	err = wflow.Save()
	utils.AssertNil(t, err)

	// the API under test is the following
	deleteWorkflowApiTest(t, fcName, HOST, PORT)

	list, err := workflow.GetAllWorkflows()
	found := false
	for _, w := range list {
		if strings.Compare(w, wflow.Name) == 0 {
			found = true
		}
	}
	utils.AssertFalse(t, found)
}

// TestAsyncInvokeWorkflow tests the REST API that executes a given function composition
func TestAsyncInvokeWorkflow(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test")
	}
	fcName := "sequence"
	fn, err := InitializePyFunction("inc", "handler", function.NewSignature().
		AddInput("input", function.Int{}).
		AddOutput("result", function.Int{}).
		Build())
	utils.AssertNilMsg(t, err, "failed to initialize function")
	wflow, err := CreateSequenceWorkflow(fn, fn, fn)
	wflow.Name = fcName
	utils.AssertNil(t, err)
	err = createWorkflowApiTest(wflow, HOST, PORT)
	if err != nil {
		fmt.Println(err)
		t.Fail()
	}

	// === this is the test ===
	params := make(map[string]interface{})
	params["input"] = 1
	invocationResult := invokeWorkflowApiTest(t, params, fcName, HOST, PORT, true)

	reqIdStruct := &function.AsyncResponse{}

	errUnmarshal := json.Unmarshal([]byte(invocationResult), reqIdStruct)
	utils.AssertNil(t, errUnmarshal)

	// wait until the result is available
	i := 0
	for {
		pollResult := pollWorkflowTest(t, reqIdStruct.ReqId, HOST, PORT)

		var compExecReport workflow.ExecutionReport
		errUnmarshalExecResult := json.Unmarshal([]byte(pollResult), &compExecReport)

		if errUnmarshalExecResult != nil {
			var unmarshalError *json.UnmarshalTypeError
			if errors.As(errUnmarshalExecResult, &unmarshalError) {
				utils.AssertFalseMsg(t, true, errUnmarshalExecResult.Error())
			}
			i++
			time.Sleep(200 * time.Millisecond)
		} else {
			result, err := GetSingleResult(&compExecReport)
			utils.AssertNilMsg(t, err, "failed to get single result")
			utils.AssertEquals(t, "4", result)
			break
		}
	}

	// here we do not use REST API
	getFC, b := workflow.Get(fcName)
	utils.AssertTrue(t, b)
	utils.AssertTrueMsg(t, wflow.Equals(getFC), "composition comparison failed")
	err = wflow.Delete()
	utils.AssertNilMsg(t, err, "failed to delete composition")
}
