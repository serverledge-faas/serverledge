package test

/// fc_test contains test that executes serverledge server-side function composition apis directly. Internally it uses __function__ REST API.
import (
	"encoding/json"
	"fmt"
	"log"
	"testing"

	"github.com/lithammer/shortuuid"
	"github.com/serverledge-faas/serverledge/internal/function"
	"github.com/serverledge-faas/serverledge/internal/workflow"
	u "github.com/serverledge-faas/serverledge/utils"
)

func TestMarshalingFunctionWorkflow(t *testing.T) {
	workflowName := "sequence"
	fn, err := InitializePyFunction("inc", "handler", function.NewSignature().
		AddInput("input", function.Int{}).
		AddOutput("result", function.Int{}).
		Build())
	u.AssertNilMsg(t, err, "failed to initialize function")
	wflow, err := CreateSequenceWorkflow(fn, fn, fn)
	wflow.Name = workflowName
	u.AssertNil(t, err)

	marshaledFunc, errMarshal := json.Marshal(wflow)
	u.AssertNilMsg(t, errMarshal, "failed to marshal composition")
	var retrieved workflow.Workflow
	errUnmarshal := json.Unmarshal(marshaledFunc, &retrieved)
	u.AssertNilMsg(t, errUnmarshal, "failed composition unmarshal")

	u.AssertTrueMsg(t, retrieved.Equals(wflow),
		fmt.Sprintf("retrieved composition is not equal to initial composition. Retrieved : %s, Expected %s ",
			retrieved.String(), wflow.String()))
}

// TestComposeFC checks the CREATE, GET and DELETE functionality of the Function Workflow
func TestComposeFC(t *testing.T) {

	if testing.Short() {
		t.Skip("Skipping integration test")
	}

	// GET1 - initially we do not have any function composition
	funcs, err := workflow.GetAllWorkflows()
	lenFuncs := len(funcs)
	u.AssertNil(t, err)

	workflowName := "test"
	// CREATE - we create a test function composition
	m := make(map[string]interface{})
	m["input"] = 0
	length := 3
	_, fArr, err := initializeSameFunctionSlice(length, "js")
	u.AssertNil(t, err)

	wflow, err := CreateSequenceWorkflow(fArr...)
	wflow.Name = workflowName
	u.AssertNil(t, err)

	err2 := wflow.Save()

	u.AssertNil(t, err2)

	// The creation is successful: we have one more function composition?
	// GET2
	funcs2, err3 := workflow.GetAllWorkflows()
	u.AssertNil(t, err3)
	u.AssertEqualsMsg(t, lenFuncs+1, len(funcs2), "creation of function failed")

	// the function is exactly the one i created?
	fun, ok := workflow.Get(workflowName)
	u.AssertTrue(t, ok)
	u.AssertTrue(t, wflow.Equals(fun))

	// DELETE
	err4 := wflow.Delete()
	u.AssertNil(t, err4)

	// The deletion is successful?
	// GET3
	funcs3, err5 := workflow.GetAllWorkflows()
	u.AssertNil(t, err5)
	u.AssertEqualsMsg(t, len(funcs3), lenFuncs, "deletion of function failed")
}

// TestInvokeFC executes a Sequential Workflow of length N, where each node executes a simple increment function.
func TestInvokeFC(t *testing.T) {

	if testing.Short() {
		t.Skip("Skipping integration test")
	}

	workflowName := "test"
	// CREATE - we create a test function composition
	length := 5
	f, fArr, err := initializeSameFunctionSlice(length, "js")
	u.AssertNil(t, err)
	wflow, err := CreateSequenceWorkflow(fArr...)
	wflow.Name = workflowName
	u.AssertNil(t, err)
	err1 := wflow.Save()
	u.AssertNil(t, err1)

	// INVOKE - we call the function composition
	params := make(map[string]interface{})
	params[f.Signature.GetInputs()[0].Name] = 0

	request := workflow.NewRequest(shortuuid.New(), wflow, params)

	resultMap, err2 := wflow.Invoke(request)
	u.AssertNil(t, err2)

	// check result
	output := resultMap.Result[f.Signature.GetOutputs()[0].Name]
	u.AssertEquals(t, length, output.(int))

	// cleaning up function composition and function
	err3 := wflow.Delete()
	u.AssertNil(t, err3)
}

// TestInvokeChoiceFC executes a Choice Workflow with N alternatives, and it executes only the second one. The functions are all the same increment function
func TestInvokeChoiceFC(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test")
	}
	workflowName := "test"
	// CREATE - we create a test function composition
	input := 2
	incJs, errJs := initializeExampleJSFunction()
	u.AssertNil(t, errJs)
	incPy, errPy := initializeExamplePyFunction()
	u.AssertNil(t, errPy)
	doublePy, errDp := InitializePyFunction("double", "handler", function.NewSignature().
		AddInput("input", function.Int{}).
		AddOutput("result", function.Int{}).Build())
	u.AssertNil(t, errDp)

	wflow, err := workflow.NewBuilder().
		AddChoiceNode(
			workflow.NewConstCondition(false),
			workflow.NewSmallerCondition(2, 1),
			workflow.NewConstCondition(true),
		).
		NextBranch(CreateSequenceWorkflow(incJs)).
		NextBranch(CreateSequenceWorkflow(incPy)).
		NextBranch(CreateSequenceWorkflow(doublePy)).
		EndChoiceAndBuild()

	wflow.Name = workflowName
	u.AssertNil(t, err)
	err1 := wflow.Save()
	u.AssertNil(t, err1)

	// this is the function that will be called
	f := doublePy

	// INVOKE - we call the function composition
	params := make(map[string]interface{})
	params[f.Signature.GetInputs()[0].Name] = input

	request := workflow.NewRequest(shortuuid.New(), wflow, params)
	resultMap, err2 := wflow.Invoke(request)
	u.AssertNil(t, err2)
	// checking the result, should be input + 1
	output := resultMap.Result[f.Signature.GetOutputs()[0].Name]
	u.AssertEquals(t, input*2, output.(int))

	// cleaning up function composition and function
	err3 := wflow.Delete()
	u.AssertNil(t, err3)
}

// TestInvokeFC_DifferentFunctions executes a Sequential Workflow of length 2, with two different functions (in different languages)
func TestInvokeFC_DifferentFunctions(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test")
	}

	workflowName := "test"
	// CREATE - we create a test function composition
	fDouble, errF1 := InitializePyFunction("double", "handler", function.NewSignature().
		AddInput("input", function.Int{}).
		AddOutput("result", function.Int{}).
		Build())
	u.AssertNil(t, errF1)

	fInc, errF2 := initializeJsFunction("inc", function.NewSignature().
		AddInput("input", function.Int{}).
		AddOutput("result", function.Int{}).
		Build())
	u.AssertNil(t, errF2)

	wflow, err := workflow.NewBuilder().
		AddSimpleNode(fDouble).
		AddSimpleNode(fInc).
		AddSimpleNode(fDouble).
		AddSimpleNode(fInc).
		Build()
	wflow.Name = workflowName

	u.AssertNil(t, err)

	err1 := wflow.Save()
	u.AssertNil(t, err1)

	// INVOKE - we call the function composition
	params := make(map[string]interface{})
	params[fDouble.Signature.GetInputs()[0].Name] = 2
	request := workflow.NewRequest(shortuuid.New(), wflow, params)
	resultMap, err2 := wflow.Invoke(request)
	if err2 != nil {
		log.Printf("%v\n", err2)
		t.FailNow()
	}
	u.AssertNil(t, err2)

	// check result
	output := resultMap.Result[fInc.Signature.GetOutputs()[0].Name]
	if output != 11 {
		t.FailNow()
	}

	u.AssertEquals(t, (2*2+1)*2+1, output.(int))

	// cleaning up function composition and function
	err3 := wflow.Delete()
	u.AssertNil(t, err3)
}

// TestInvokeFC_BroadcastFanOut executes a Parallel Workflow with N parallel branches
func TestInvokeFC_BroadcastFanOut(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test")
	}

	workflowName := "testBrFO"
	// CREATE - we create a test function composition
	fDouble, errF1 := InitializePyFunction("double", "handler", function.NewSignature().
		AddInput("input", function.Int{}).
		AddOutput("result", function.Int{}).
		Build())
	u.AssertNil(t, errF1)

	width := 3
	wflow, err := CreateBroadcastWorkflow(func() (*workflow.Workflow, error) { return CreateSequenceWorkflow(fDouble) }, width)
	wflow.Name = workflowName
	u.AssertNil(t, err)

	err1 := wflow.Save()
	u.AssertNil(t, err1)

	// INVOKE - we call the function composition
	params := make(map[string]interface{})
	params[fDouble.Signature.GetInputs()[0].Name] = 1
	request := workflow.NewRequest(shortuuid.New(), wflow, params)
	resultMap, err2 := wflow.Invoke(request)
	u.AssertNil(t, err2)

	// check multiple result
	output := resultMap.Result

	u.AssertNonNil(t, output)
	for i := 0; i < width; i++ {
		currOutput := output[fmt.Sprintf("%d", i)].(map[string]interface{})
		u.AssertEquals(t, 2, currOutput["result"])
	}

	// cleaning up function composition and functions
	//err3 := workflow.Delete()
	//u.AssertNil(t, err3)
}

// TestInvokeFC_Concurrent executes concurrently m times a Sequential Workflow of length N, where each node executes a simple increment function.
func TestInvokeFC_Concurrent(t *testing.T) {

	if testing.Short() {
		t.Skip("Skipping integration test")
	}

	workflowName := "test"
	// CREATE - we create a test function composition
	length := 5
	f, _, err := initializeSameFunctionSlice(length, "py")
	u.AssertNil(t, err)
	builder := workflow.NewBuilder()
	for i := 0; i < length; i++ {
		builder.AddSimpleNodeWithId(f, fmt.Sprintf("simple %d", i))
	}
	wflow, err := builder.Build()
	wflow.Name = workflowName
	u.AssertNil(t, err)

	err1 := wflow.Save()
	u.AssertNil(t, err1)

	concurrencyLevel := 3
	start := make(chan int)
	results := make(map[int]chan interface{})
	errors := make(map[int]chan error)
	// initialize channels
	for i := 0; i < concurrencyLevel; i++ {
		results[i] = make(chan interface{})
		errors[i] = make(chan error)
	}

	for i := 0; i < concurrencyLevel; i++ {
		resultChan := results[i]
		errChan := errors[i]
		// INVOKE - we call the function composition concurrently m times
		go func(i int, resultChan chan interface{}, errChan chan error, start chan int) {
			params := make(map[string]interface{})
			params[f.Signature.GetInputs()[0].Name] = i

			request := workflow.NewRequest(fmt.Sprintf("goroutine_%d", i), wflow, params)
			// wait until all goroutines are ready
			<-start
			// return error
			resultMap, err2 := wflow.Invoke(request)
			errChan <- err2
			// return result
			output := resultMap.Result[f.Signature.GetOutputs()[0].Name]
			resultChan <- output
		}(i, resultChan, errChan, start)
	}
	// let's start all the goroutines at the same time
	for i := 0; i < concurrencyLevel; i++ {
		start <- 1
	}

	// and wait for errors (hopefully not) and results
	for _, e := range errors {
		maybeError := <-e
		u.AssertNilMsg(t, maybeError, "error in goroutine")
	}

	for i, r := range results {
		output := <-r
		u.AssertEqualsMsg(t, length+i, output.(int), fmt.Sprintf("output of goroutine %d is wrong", i))
	}

	// cleaning up function composition and function
	err3 := wflow.Delete()
	u.AssertNil(t, err3)
}

// TestInvokeFC_ScatterFanOut executes a Parallel Workflow with N parallel branches
func TestInvokeFC_ScatterFanOut(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test")
	}
	//for i := 0; i < 1; i++ {

	workflowName := "test"
	// CREATE - we create a test function composition
	fDouble, errF1 := InitializePyFunction("double", "handler", function.NewSignature().
		AddInput("input", function.Int{}).
		AddOutput("result", function.Int{}).
		Build())
	u.AssertNil(t, errF1)

	width := 3
	wflow, err := CreateScatterSingleFunctionWorkflow(fDouble, width)
	wflow.Name = workflowName
	u.AssertNil(t, err)

	err1 := wflow.Save()
	u.AssertNil(t, err1)

	// INVOKE - we call the function composition
	params := make(map[string]interface{})
	params[fDouble.Signature.GetInputs()[0].Name] = []int{1, 2, 3}
	request := workflow.NewRequest(shortuuid.New(), wflow, params)
	resultMap, err2 := wflow.Invoke(request)
	u.AssertNil(t, err2)

	// check multiple result
	output := resultMap.Result
	fmt.Println(output)
	u.AssertNonNil(t, output)
	for i := 0; i < width; i++ {
		currOutput := output[fmt.Sprintf("%d", i)].(map[string]interface{})
		u.AssertEquals(t, (i+1)*2, currOutput["result"].(int))
	}

	// cleaning up function composition and functions
	err3 := wflow.Delete()
	u.AssertNil(t, err3)
}

func TestInvokeSieveChoice(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test")
	}
	workflowName := "test"
	input := 13
	sieveJs, errJs := initializeJsFunction("sieve", function.NewSignature().
		AddInput("n", function.Int{}).
		AddOutput("N", function.Int{}).
		AddOutput("Primes", function.Array[function.Int]{}).
		Build())
	u.AssertNil(t, errJs)

	isPrimePy, errPy := InitializePyFunction("isprimeWithNumber", "handler", function.NewSignature().
		AddInput("n", function.Int{}).
		AddOutput("IsPrime", function.Bool{}).
		AddOutput("n", function.Int{}).
		Build())
	u.AssertNil(t, errPy)

	incPy, errDp := InitializePyFunction("inc", "handler", function.NewSignature().
		AddInput("input", function.Int{}).
		AddOutput("result", function.Int{}).Build())
	u.AssertNil(t, errDp)

	wflow, err := workflow.NewBuilder().
		AddSimpleNode(isPrimePy).
		AddChoiceNode(
			workflow.NewEqParamCondition(workflow.NewParam("IsPrime"), workflow.NewValue(true)),
			workflow.NewEqParamCondition(workflow.NewParam("IsPrime"), workflow.NewValue(false)),
		).
		NextBranch(CreateSequenceWorkflow(sieveJs)).
		NextBranch(CreateSequenceWorkflow(incPy)).
		EndChoiceAndBuild()
	wflow.Name = workflowName

	u.AssertNil(t, err)
	err1 := wflow.Save()
	u.AssertNil(t, err1)

	// INVOKE - we call the function composition
	params := make(map[string]interface{})
	params[isPrimePy.Signature.GetInputs()[0].Name] = input

	request := workflow.NewRequest(shortuuid.New(), wflow, params)
	resultMap, err2 := wflow.Invoke(request)
	u.AssertNil(t, err2)

	// checking the result
	output := resultMap.Result[sieveJs.Signature.GetOutputs()[1].Name]
	slice, err := u.ConvertToSlice(output)
	u.AssertNil(t, err)

	res, err := u.ConvertInterfaceToSpecificSlice[float64](slice)
	u.AssertNil(t, err)

	u.AssertSliceEqualsMsg[float64](t, []float64{2, 3, 5, 7, 11, 13}, res, "output is wrong")

	// cleaning up function composition and function
	err3 := wflow.Delete()
	u.AssertNil(t, err3)
}

func TestInvokeWorkflowError(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test")
	}
	workflowName := "error"

	incPy, errDp := InitializePyFunction("inc", "handler", function.NewSignature().
		AddInput("input", function.Int{}).
		AddOutput("result", function.Int{}).Build())
	u.AssertNil(t, errDp)

	wflow, err := workflow.NewBuilder().
		AddChoiceNode(
			workflow.NewEqParamCondition(workflow.NewParam("NonExistentParam"), workflow.NewValue(true)),
			workflow.NewEqCondition(2, 3),
		).
		NextBranch(CreateSequenceWorkflow(incPy)).
		EndChoiceAndBuild()
	wflow.Name = workflowName
	u.AssertNil(t, err)
	err1 := wflow.Save()
	u.AssertNil(t, err1)

	// INVOKE - we call the function composition
	params := make(map[string]interface{})
	params[incPy.Signature.GetInputs()[0].Name] = 1

	request := workflow.NewRequest(shortuuid.New(), wflow, params)
	_, err2 := wflow.Invoke(request)
	u.AssertNonNil(t, err2)
}

func TestInvokeWorkflowFailAndSucceed(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test")
	}

	wflow, err := workflow.NewBuilder().
		AddChoiceNode(
			workflow.NewEqParamCondition(workflow.NewParam("value"), workflow.NewValue(1)),
			workflow.NewConstCondition(true),
		).
		NextBranch(workflow.NewBuilder().AddSucceedNodeAndBuild("everything ok")).
		NextBranch(workflow.NewBuilder().AddFailNodeAndBuild("FakeError", "This should be an error")).
		EndChoiceAndBuild()
	u.AssertNil(t, err)
	wflow.Name = "fail_succeed"
	err1 := wflow.Save()
	u.AssertNil(t, err1)

	// First run: Success

	// INVOKE - we call the function composition
	params := make(map[string]interface{})
	params["value"] = 1

	request := workflow.NewRequest(shortuuid.New(), wflow, params)
	resultMap, errInvoke1 := wflow.Invoke(request)
	u.AssertNilMsg(t, errInvoke1, "error while invoking the branch (succeed)")

	result, err := GetIntSingleResult(&resultMap)
	u.AssertNilMsg(t, err, "Result not found")
	u.AssertEquals(t, 1, result)

	// Second run: Fail
	params2 := make(map[string]interface{})
	params2["value"] = 2

	request2 := workflow.NewRequest(shortuuid.New(), wflow, params2)
	resultMap2, errInvoke2 := wflow.Invoke(request2)
	u.AssertNilMsg(t, errInvoke2, "error while invoking the branch (fail)")

	valueError, found := resultMap2.Result["FakeError"]
	u.AssertTrueMsg(t, found, "FakeError not found")
	causeStr, ok := valueError.(string)

	u.AssertTrueMsg(t, ok, "cause value is not a string")
	u.AssertEquals(t, "This should be an error", causeStr)
}

func TestInvokeWorkflowPassDoNothing(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test")
	}

	incPy, errDp := InitializePyFunction("inc", "handler", function.NewSignature().
		AddInput("input", function.Int{}).
		AddOutput("result", function.Int{}).Build())
	u.AssertNil(t, errDp)
	wflow, err := workflow.NewBuilder().
		AddSimpleNode(incPy).
		AddPassNode(""). // this should not do nothing
		AddSimpleNode(incPy).
		Build()
	wflow.Name = "pass_do_nothing"
	u.AssertNil(t, err)

	err1 := wflow.Save()
	u.AssertNil(t, err1)

	params := make(map[string]interface{})
	params["input"] = 1

	request := workflow.NewRequest(shortuuid.New(), wflow, params)
	resultMap, errInvoke1 := wflow.Invoke(request)
	u.AssertNilMsg(t, errInvoke1, "error while invoking the composition with pass node")

	result, err := GetIntSingleResult(&resultMap)
	u.AssertNilMsg(t, err, "Result not found")
	u.AssertEquals(t, 3, result)
}
