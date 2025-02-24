package test

import (
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/grussorusso/serverledge/internal/fc"
	"github.com/grussorusso/serverledge/internal/function"
	"github.com/grussorusso/serverledge/utils"
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
	//utils.AssertTrueMsg(t, fc.IsEmptyPartialDataCache(), "partial data cache is not empty")
}

// TestCreateComposition tests the compose REST API that creates a new function composition
func TestCreateComposition(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test")
	}
	fcName := "sequence"
	fn, err := InitializePyFunction("inc", "handler", function.NewSignature().
		AddInput("input", function.Int{}).
		AddOutput("result", function.Int{}).
		Build())
	utils.AssertNilMsg(t, err, "failed to initialize function")
	dag, err := fc.CreateSequenceDag(fn, fn, fn)
	dag.Name = fcName
	utils.AssertNil(t, err)
	err = createCompositionApiTest(dag, HOST, PORT)
	if err != nil {
		fmt.Println(err)
		t.Fail()
	}

	// here we do not use REST API
	getFC, b := fc.GetFC(fcName)
	utils.AssertTrue(t, b)
	utils.AssertTrueMsg(t, dag.Equals(getFC), "composition comparison failed")
	err = dag.Delete()
	utils.AssertNilMsg(t, err, "failed to delete composition")

}

// TestInvokeComposition tests the REST API that executes a given function composition
func TestInvokeComposition(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test")
	}
	fcName := "sequence"
	fn, err := initializeJsFunction("inc", function.NewSignature().
		AddInput("input", function.Int{}).
		AddOutput("result", function.Int{}).
		Build())
	utils.AssertNilMsg(t, err, "failed to initialize function")
	dag, err := fc.CreateSequenceDag(fn, fn, fn)
	dag.Name = fcName
	utils.AssertNil(t, err)
	err = createCompositionApiTest(dag, HOST, PORT)
	if err != nil {
		fmt.Println(err)
		t.Fail()
	}

	// === this is the test ===
	params := make(map[string]interface{})
	params["input"] = 1
	invokeCompositionApiTest(t, params, fcName, HOST, PORT, false)

	// here we do not use REST API
	getFC, b := fc.GetFC(fcName)
	utils.AssertTrue(t, b)
	utils.AssertTrueMsg(t, dag.Equals(getFC), "composition comparison failed")
	err = dag.Delete()
	utils.AssertNilMsg(t, err, "failed to delete composition")

}

// TestInvokeComposition tests the REST API that executes a given function composition
func TestInvokeComposition_DifferentFunctions(t *testing.T) {
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
	dag, err := fc.CreateSequenceDag(fnPy, fnJs, fnPy, fnJs)
	dag.Name = fcName
	utils.AssertNil(t, err)
	err = createCompositionApiTest(dag, HOST, PORT)
	if err != nil {
		fmt.Println(err)
		t.Fail()
	}

	// === this is the test ===
	params := make(map[string]interface{})
	params["input"] = 1
	invokeCompositionApiTest(t, params, fcName, HOST, PORT, false)

	// here we do not use REST API
	getFC, b := fc.GetFC(fcName)
	utils.AssertTrue(t, b)
	utils.AssertTrueMsg(t, dag.Equals(getFC), "composition comparison failed")
	err = dag.Delete()
	utils.AssertNilMsg(t, err, "failed to delete composition")

}

// TestDeleteComposition tests the compose REST API that deletes a function composition
func TestDeleteComposition(t *testing.T) {
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
	dag, err := fc.CreateSequenceDag(fn, db, fn)
	dag.Name = fcName
	utils.AssertNil(t, err)

	err = dag.SaveToEtcd()
	utils.AssertNil(t, err)

	// the API under test is the following
	deleteCompositionApiTest(t, fcName, HOST, PORT) // TODO: check success

	// delete the container when not used
	// deleteApiTest(t, fn.Name, HOST, PORT) // the function has already been deleted during composition deletion

	// utils.AssertTrueMsg(t, node.ArePoolsEmptyInThisNode(), "container pools are not empty after the end of test")
	// utils.AssertTrueMsg(t, fc.IsEmptyPartialDataCache(), "partial data cache is not empty")
}

// TestAsyncInvokeComposition tests the REST API that executes a given function composition
func TestAsyncInvokeComposition(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test")
	}
	fcName := "sequence"
	fn, err := InitializePyFunction("inc", "handler", function.NewSignature().
		AddInput("input", function.Int{}).
		AddOutput("result", function.Int{}).
		Build())
	utils.AssertNilMsg(t, err, "failed to initialize function")
	dag, err := fc.CreateSequenceDag(fn, fn, fn)
	dag.Name = fcName
	utils.AssertNil(t, err)
	err = createCompositionApiTest(dag, HOST, PORT)
	if err != nil {
		fmt.Println(err)
		t.Fail()
	}

	// === this is the test ===
	params := make(map[string]interface{})
	params["input"] = 1
	invocationResult := invokeCompositionApiTest(t, params, fcName, HOST, PORT, true)

	reqIdStruct := &function.AsyncResponse{}

	errUnmarshal := json.Unmarshal([]byte(invocationResult), reqIdStruct)
	utils.AssertNil(t, errUnmarshal)

	// wait until the result is available
	i := 0
	for {
		pollResult := pollCompositionTest(t, reqIdStruct.ReqId, HOST, PORT)

		var compExecReport fc.CompositionExecutionReport
		errUnmarshalExecResult := json.Unmarshal([]byte(pollResult), &compExecReport)

		if errUnmarshalExecResult != nil {
			var unmarshalError *json.UnmarshalTypeError
			if errors.As(errUnmarshalExecResult, &unmarshalError) {
				utils.AssertFalseMsg(t, true, errUnmarshalExecResult.Error())
			}
			i++
			time.Sleep(200 * time.Millisecond)
		} else {
			result, err := compExecReport.GetSingleResult()
			utils.AssertNilMsg(t, err, "failed to get single result")
			utils.AssertEquals(t, "4", result)
			break
		}
	}

	// here we do not use REST API
	getFC, b := fc.GetFC(fcName)
	utils.AssertTrue(t, b)
	utils.AssertTrueMsg(t, dag.Equals(getFC), "composition comparison failed")
	err = dag.Delete()
	utils.AssertNilMsg(t, err, "failed to delete composition")
}
