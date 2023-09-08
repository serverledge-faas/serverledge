package test

import (
	"encoding/json"
	"fmt"
	"github.com/grussorusso/serverledge/internal/fc"
	"github.com/grussorusso/serverledge/internal/function"
	"github.com/grussorusso/serverledge/internal/node"
	"github.com/grussorusso/serverledge/utils"
	"testing"
	"time"
)

// TestContainerPool executes repeatedly different functions (**not compositions**) to verify the container pool
func TestContainerPool(t *testing.T) {
	if !IntegrationTest {
		t.Skip()
	}
	// creating inc and double functions
	funcs := []string{"inc", "double"}
	for _, name := range funcs {
		fn, err := InitializePyFunction(name, "handler", function.NewSignature().
			AddInput("input", function.Int{}).
			AddOutput("result", function.Int{}).
			Build())
		utils.AssertNil(t, err)

		createApiTest(t, fn, HOST, PORT)
	}
	// getting functions
	functionNames := getFunctionApiTest(t, HOST, PORT)
	utils.AssertSliceEquals(t, []string{"double", "inc"}, functionNames)
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
	if !IntegrationTest {
		t.Skip()
	}
	fcName := "sequence"
	fn, err := InitializePyFunction("inc", "handler", function.NewSignature().
		AddInput("input", function.Int{}).
		AddOutput("result", function.Int{}).
		Build())
	utils.AssertNilMsg(t, err, "failed to initialize function")
	dag, err := fc.CreateSequenceDag(fn, fn, fn)
	composition := fc.NewFC(fcName, *dag, []*function.Function{fn}, true)
	createCompositionApiTest(t, &composition, HOST, PORT)

	// verifies the function exists (using function REST API)
	functionNames := getFunctionApiTest(t, HOST, PORT)
	utils.AssertSliceEquals(t, []string{"inc"}, functionNames)

	// here we do not use REST API
	getFC, b := fc.GetFC(fcName)
	utils.AssertTrue(t, b)
	utils.AssertTrueMsg(t, composition.Equals(getFC), "composition comparison failed")
	err = composition.Delete()
	utils.AssertNilMsg(t, err, "failed to delete composition")

	// verifies the function does not exists  (using function REST API)
	functionNames = getFunctionApiTest(t, HOST, PORT)
	utils.AssertSliceEquals(t, []string{}, functionNames)

	//utils.AssertTrueMsg(t, fc.IsEmptyPartialDataCache(), "partial data cache is not empty")
}

// TestInvokeComposition tests the REST API that executes a given function composition
func TestInvokeComposition(t *testing.T) {
	if !IntegrationTest {
		t.Skip()
	}
	fcName := "sequence"
	fn, err := initializeJsFunction("inc", function.NewSignature().
		AddInput("input", function.Int{}).
		AddOutput("result", function.Int{}).
		Build())
	utils.AssertNilMsg(t, err, "failed to initialize function")
	dag, err := fc.CreateSequenceDag(fn, fn, fn)
	composition := fc.NewFC(fcName, *dag, []*function.Function{fn}, true)
	createCompositionApiTest(t, &composition, HOST, PORT)

	// verifies the function exists (using function REST API)
	functionNames := getFunctionApiTest(t, HOST, PORT)
	utils.AssertSliceEquals(t, []string{"inc"}, functionNames)

	// === this is the test ===
	params := make(map[string]interface{})
	params["input"] = 1
	invocationResult := invokeCompositionApiTest(t, params, fcName, HOST, PORT, false)
	fmt.Println(invocationResult)

	// here we do not use REST API
	getFC, b := fc.GetFC(fcName)
	utils.AssertTrue(t, b)
	utils.AssertTrueMsg(t, composition.Equals(getFC), "composition comparison failed")
	err = composition.Delete()
	utils.AssertNilMsg(t, err, "failed to delete composition")

	// verifies the function does not exists  (using function REST API)
	functionNames = getFunctionApiTest(t, HOST, PORT)
	utils.AssertSliceEquals(t, []string{}, functionNames)

	//utils.AssertTrueMsg(t, fc.IsEmptyPartialDataCache(), "partial data cache is not empty")
}

// TestInvokeComposition tests the REST API that executes a given function composition
func TestInvokeComposition_DifferentFunctions(t *testing.T) {
	if !IntegrationTest {
		t.Skip()
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
	composition := fc.NewFC(fcName, *dag, []*function.Function{fnPy, fnJs}, true)
	createCompositionApiTest(t, &composition, HOST, PORT)

	// verifies the function exists (using function REST API)
	functionNames := getFunctionApiTest(t, HOST, PORT)
	utils.AssertEquals(t, 2, len(functionNames))

	// === this is the test ===
	params := make(map[string]interface{})
	params["input"] = 1
	invocationResult := invokeCompositionApiTest(t, params, fcName, HOST, PORT, false)
	fmt.Println(invocationResult)

	// here we do not use REST API
	getFC, b := fc.GetFC(fcName)
	utils.AssertTrue(t, b)
	utils.AssertTrueMsg(t, composition.Equals(getFC), "composition comparison failed")
	err = composition.Delete()
	utils.AssertNilMsg(t, err, "failed to delete composition")

	// verifies the function does not exists  (using function REST API)
	functionNames = getFunctionApiTest(t, HOST, PORT)
	utils.AssertSliceEquals(t, []string{}, functionNames)

	//utils.AssertTrueMsg(t, fc.IsEmptyPartialDataCache(), "partial data cache is not empty")
}

// TestDeleteComposition tests the compose REST API that deletes a function composition
func TestDeleteComposition(t *testing.T) {
	if !IntegrationTest {
		t.Skip()
	}
	fcName := "sequence"
	fn, err := InitializePyFunction("inc", "handler", function.NewSignature().
		AddInput("input", function.Int{}).
		AddOutput("result", function.Int{}).
		Build())
	db, err := InitializePyFunction("double", "handler", function.NewSignature().
		AddInput("input", function.Int{}).
		AddOutput("result", function.Int{}).
		Build())
	utils.AssertNilMsg(t, err, "failed to initialize function")
	dag, err := fc.CreateSequenceDag(fn, db, fn)
	for _, b := range []bool{true, false} {
		composition := fc.NewFC(fcName, *dag, []*function.Function{fn, db}, b)
		err = composition.SaveToEtcd()
		utils.AssertNil(t, err)

		// verifies the function exists (using function REST API)
		functionNames := getFunctionApiTest(t, HOST, PORT)
		utils.AssertSliceEquals(t, []string{"double", "inc"}, functionNames)

		// verifies the function composition exists (using function composition REST API)
		compositionNames := getCompositionsApiTest(t, HOST, PORT)
		utils.AssertSliceEquals(t, []string{"sequence"}, compositionNames)

		// the API under test is the following
		deleteCompositionApiTest(t, fcName, HOST, PORT)

		// verifies the function composition doen't exists (using function composition REST API)
		compositionNames = getCompositionsApiTest(t, HOST, PORT)
		utils.AssertSliceEquals(t, []string{}, compositionNames)

		functionNames = getFunctionApiTest(t, HOST, PORT)
		if composition.RemoveFnOnDeletion {
			// verifies the function does not exists  (using function REST API)
			utils.AssertSliceEquals(t, []string{}, functionNames)
		} else {
			// verifies the function exists  (using function REST API)
			utils.AssertSliceEquals(t, []string{"double", "inc"}, functionNames)
		}
	}

	// delete the container when not used
	deleteApiTest(t, fn.Name, HOST, PORT)
	node.ShutdownWarmContainersFor(fn)

	// utils.AssertTrueMsg(t, node.ArePoolsEmptyInThisNode(), "container pools are not empty after the end of test")
	// utils.AssertTrueMsg(t, fc.IsEmptyPartialDataCache(), "partial data cache is not empty")
}

// TestAsyncInvokeComposition tests the REST API that executes a given function composition
func TestAsyncInvokeComposition(t *testing.T) {
	if !IntegrationTest {
		t.Skip()
	}
	fcName := "sequence"
	fn, err := InitializePyFunction("inc", "handler", function.NewSignature().
		AddInput("input", function.Int{}).
		AddOutput("result", function.Int{}).
		Build())
	utils.AssertNilMsg(t, err, "failed to initialize function")
	dag, err := fc.CreateSequenceDag(fn, fn, fn)
	composition := fc.NewFC(fcName, *dag, []*function.Function{fn}, true)
	createCompositionApiTest(t, &composition, HOST, PORT)

	// === this is the test ===
	params := make(map[string]interface{})
	params["input"] = 1
	invocationResult := invokeCompositionApiTest(t, params, fcName, HOST, PORT, true)
	fmt.Println(invocationResult)

	reqIdStruct := &struct {
		ReqId string
	}{}

	errUnmarshal := json.Unmarshal([]byte(invocationResult), reqIdStruct)
	utils.AssertNil(t, errUnmarshal)

	// wait until the result is available
	i := 0
	for {
		pollResult := pollCompositionTest(t, reqIdStruct.ReqId, HOST, PORT)
		fmt.Println(pollResult)

		compExecReport := &fc.CompositionExecutionReport{}
		errUnmarshalExecResult := json.Unmarshal([]byte(pollResult), compExecReport)

		result := compExecReport.GetSingleResult()
		if errUnmarshalExecResult != nil {
			i++
			fmt.Printf("Attempt %d - Result not available - retrying after 200 ms\n", i)
			time.Sleep(200 * time.Millisecond)
		} else {
			utils.AssertEquals(t, "4", result)
			break
		}
	}

	// here we do not use REST API
	getFC, b := fc.GetFC(fcName)
	utils.AssertTrue(t, b)
	utils.AssertTrueMsg(t, composition.Equals(getFC), "composition comparison failed")
	err = composition.Delete()
	utils.AssertNilMsg(t, err, "failed to delete composition")
	// removing functions container to release resources

	for _, fun := range composition.Functions {
		// Delete local warm containers
		node.ShutdownWarmContainersFor(fun)
	}
	//utils.AssertTrueMsg(t, fc.IsEmptyPartialDataCache(), "partial data cache is not empty")
}
