package test

/// fc_test contains test that executes serverledge server-side function composition apis directly. Internally it uses __function__ REST API.
import (
	"encoding/json"
	"fmt"
	"golang.org/x/exp/slices"
	"log"
	"testing"

	"github.com/grussorusso/serverledge/internal/fc"
	"github.com/grussorusso/serverledge/internal/function"
	u "github.com/grussorusso/serverledge/utils"
	"github.com/lithammer/shortuuid"
)

func TestMarshalingFunctionComposition(t *testing.T) {
	fcName := "sequence"
	fn, err := InitializePyFunction("inc", "handler", function.NewSignature().
		AddInput("input", function.Int{}).
		AddOutput("result", function.Int{}).
		Build())
	u.AssertNilMsg(t, err, "failed to initialize function")
	workflow, err := fc.CreateSequenceDag(fn, fn, fn)
	workflow.Name = fcName
	u.AssertNil(t, err)

	marshaledFunc, errMarshal := json.Marshal(workflow)
	u.AssertNilMsg(t, errMarshal, "failed to marshal composition")
	var retrieved fc.Workflow
	errUnmarshal := json.Unmarshal(marshaledFunc, &retrieved)
	u.AssertNilMsg(t, errUnmarshal, "failed composition unmarshal")

	u.AssertTrueMsg(t, retrieved.Equals(workflow),
		fmt.Sprintf("retrieved composition is not equal to initial composition. Retrieved : %s, Expected %s ",
			retrieved.String(), workflow.String()))
}

// TestComposeFC checks the CREATE, GET and DELETE functionality of the Function Composition
func TestComposeFC(t *testing.T) {

	if testing.Short() {
		t.Skip("Skipping integration test")
	}

	// GET1 - initially we do not have any function composition
	funcs, err := fc.GetAllFC()
	lenFuncs := len(funcs)
	u.AssertNil(t, err)

	fcName := "test"
	// CREATE - we create a test function composition
	m := make(map[string]interface{})
	m["input"] = 0
	length := 3
	_, fArr, err := initializeSameFunctionSlice(length, "js")
	u.AssertNil(t, err)

	workflow, err := fc.CreateSequenceDag(fArr...)
	workflow.Name = fcName
	u.AssertNil(t, err)

	err2 := workflow.SaveToEtcd()

	u.AssertNil(t, err2)

	// The creation is successful: we have one more function composition?
	// GET2
	funcs2, err3 := fc.GetAllFC()
	u.AssertNil(t, err3)
	u.AssertEqualsMsg(t, lenFuncs+1, len(funcs2), "creation of function failed")

	// the function is exactly the one i created?
	fun, ok := fc.GetFC(fcName)
	u.AssertTrue(t, ok)
	u.AssertTrue(t, workflow.Equals(fun))

	// DELETE
	err4 := workflow.Delete()
	u.AssertNil(t, err4)

	// The deletion is successful?
	// GET3
	funcs3, err5 := fc.GetAllFC()
	u.AssertNil(t, err5)
	u.AssertEqualsMsg(t, len(funcs3), lenFuncs, "deletion of function failed")
}

// TestInvokeFC executes a Sequential Workflow of length N, where each node executes a simple increment function.
func TestInvokeFC(t *testing.T) {

	if testing.Short() {
		t.Skip("Skipping integration test")
	}

	fcName := "test"
	// CREATE - we create a test function composition
	length := 5
	f, fArr, err := initializeSameFunctionSlice(length, "js")
	u.AssertNil(t, err)
	fcomp, errDag := fc.CreateSequenceDag(fArr...)
	fcomp.Name = fcName
	u.AssertNil(t, errDag)
	err1 := fcomp.SaveToEtcd()
	u.AssertNil(t, err1)

	// INVOKE - we call the function composition
	params := make(map[string]interface{})
	params[f.Signature.GetInputs()[0].Name] = 0

	request := fc.NewCompositionRequest(shortuuid.New(), fcomp, params)

	resultMap, err2 := fcomp.Invoke(request)
	u.AssertNil(t, err2)

	// check result
	output := resultMap.Result[f.Signature.GetOutputs()[0].Name]
	u.AssertEquals(t, length, output.(int))

	// cleaning up function composition and function
	err3 := fcomp.Delete()
	u.AssertNil(t, err3)
}

// TestInvokeChoiceFC executes a Choice Workflow with N alternatives, and it executes only the second one. The functions are all the same increment function
func TestInvokeChoiceFC(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test")
	}
	fcName := "test"
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

	fcomp, errDag := fc.NewDagBuilder().
		AddChoiceNode(
			fc.NewConstCondition(false),
			fc.NewSmallerCondition(2, 1),
			fc.NewConstCondition(true),
		).
		NextBranch(fc.CreateSequenceDag(incJs)).
		NextBranch(fc.CreateSequenceDag(incPy)).
		NextBranch(fc.CreateSequenceDag(doublePy)).
		EndChoiceAndBuild()

	fcomp.Name = fcName
	u.AssertNil(t, errDag)
	err1 := fcomp.SaveToEtcd()
	u.AssertNil(t, err1)

	// this is the function that will be called
	f := doublePy

	// INVOKE - we call the function composition
	params := make(map[string]interface{})
	params[f.Signature.GetInputs()[0].Name] = input

	request := fc.NewCompositionRequest(shortuuid.New(), fcomp, params)
	resultMap, err2 := fcomp.Invoke(request)
	u.AssertNil(t, err2)
	// checking the result, should be input + 1
	output := resultMap.Result[f.Signature.GetOutputs()[0].Name]
	u.AssertEquals(t, input*2, output.(int))

	// cleaning up function composition and function
	err3 := fcomp.Delete()
	u.AssertNil(t, err3)
}

// TestInvokeFC_DifferentFunctions executes a Sequential Workflow of length 2, with two different functions (in different languages)
func TestInvokeFC_DifferentFunctions(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test")
	}

	fcName := "test"
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

	fcomp, errDag := fc.NewDagBuilder().
		AddSimpleNode(fDouble).
		AddSimpleNode(fInc).
		AddSimpleNode(fDouble).
		AddSimpleNode(fInc).
		Build()
	fcomp.Name = fcName

	u.AssertNil(t, errDag)

	err1 := fcomp.SaveToEtcd()
	u.AssertNil(t, err1)

	// INVOKE - we call the function composition
	params := make(map[string]interface{})
	params[fDouble.Signature.GetInputs()[0].Name] = 2
	request := fc.NewCompositionRequest(shortuuid.New(), fcomp, params)
	resultMap, err2 := fcomp.Invoke(request)
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
	err3 := fcomp.Delete()
	u.AssertNil(t, err3)
}

// TestInvokeFC_BroadcastFanOut executes a Parallel Workflow with N parallel branches
func TestInvokeFC_BroadcastFanOut(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test")
	}

	fcName := "testBrFO"
	// CREATE - we create a test function composition
	fDouble, errF1 := InitializePyFunction("double", "handler", function.NewSignature().
		AddInput("input", function.Int{}).
		AddOutput("result", function.Int{}).
		Build())
	u.AssertNil(t, errF1)

	width := 3
	fcomp, errDag := fc.CreateBroadcastDag(func() (*fc.Workflow, error) { return fc.CreateSequenceDag(fDouble) }, width)
	fcomp.Name = fcName
	u.AssertNil(t, errDag)

	err1 := fcomp.SaveToEtcd()
	u.AssertNil(t, err1)

	// INVOKE - we call the function composition
	params := make(map[string]interface{})
	params[fDouble.Signature.GetInputs()[0].Name] = 1
	request := fc.NewCompositionRequest(shortuuid.New(), fcomp, params)
	resultMap, err2 := fcomp.Invoke(request)
	u.AssertNil(t, err2)

	// check multiple result
	output := resultMap.Result
	u.AssertNonNil(t, output)
	for _, res := range output {
		u.AssertEquals(t, 2, res.(int))
	}

	// cleaning up function composition and functions
	//err3 := fcomp.Delete()
	//u.AssertNil(t, err3)
}

// TestInvokeFC_Concurrent executes concurrently m times a Sequential Workflow of length N, where each node executes a simple increment function.
func TestInvokeFC_Concurrent(t *testing.T) {

	if testing.Short() {
		t.Skip("Skipping integration test")
	}

	fcName := "test"
	// CREATE - we create a test function composition
	length := 5
	f, _, err := initializeSameFunctionSlice(length, "py")
	u.AssertNil(t, err)
	builder := fc.NewDagBuilder()
	for i := 0; i < length; i++ {
		builder.AddSimpleNodeWithId(f, fmt.Sprintf("simple %d", i))
	}
	fcomp, errDag := builder.Build()
	fcomp.Name = fcName
	u.AssertNil(t, errDag)

	err1 := fcomp.SaveToEtcd()
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

			request := fc.NewCompositionRequest(fmt.Sprintf("goroutine_%d", i), fcomp, params)
			// wait until all goroutines are ready
			<-start
			// return error
			resultMap, err2 := fcomp.Invoke(request)
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
	err3 := fcomp.Delete()
	u.AssertNil(t, err3)
}

// TestInvokeFC_ScatterFanOut executes a Parallel Workflow with N parallel branches
func TestInvokeFC_ScatterFanOut(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test")
	}
	//for i := 0; i < 1; i++ {

	fcName := "test"
	// CREATE - we create a test function composition
	fDouble, errF1 := InitializePyFunction("double", "handler", function.NewSignature().
		AddInput("input", function.Int{}).
		AddOutput("result", function.Int{}).
		Build())
	u.AssertNil(t, errF1)

	width := 3
	fcomp, errDag := fc.CreateScatterSingleFunctionDag(fDouble, width)
	fcomp.Name = fcName
	u.AssertNil(t, errDag)

	err1 := fcomp.SaveToEtcd()
	u.AssertNil(t, err1)

	// INVOKE - we call the function composition
	params := make(map[string]interface{})
	params[fDouble.Signature.GetInputs()[0].Name] = []int{1, 2, 3}
	request := fc.NewCompositionRequest(shortuuid.New(), fcomp, params)
	resultMap, err2 := fcomp.Invoke(request)
	u.AssertNil(t, err2)

	// check multiple result
	output := resultMap.Result
	u.AssertNonNil(t, output)
	for _, res := range output {
		genericSlice, ok := res.([]interface{})
		u.AssertTrue(t, ok)
		specificSlice, err := u.ConvertToSpecificSlice[int](genericSlice)
		u.AssertNil(t, err)
		// check that the result has exactly 3 elements and they are 2,4,6
		if len(specificSlice) != 3 {
			fmt.Println("the length of the slice is 3")
			t.Fail()
		}
		for _, i := range []int{2, 4, 6} {
			if !slices.Contains(specificSlice, i) {
				fmt.Printf("the result slice does not contain %d", i)
				t.Fail()
			}
		}
	}

	// cleaning up function composition and functions
	err3 := fcomp.Delete()
	u.AssertNil(t, err3)
}

func TestInvokeSieveChoice(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test")
	}
	fcName := "test"
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

	fcomp, errDag := fc.NewDagBuilder().
		AddSimpleNode(isPrimePy).
		AddChoiceNode(
			fc.NewEqParamCondition(fc.NewParam("IsPrime"), fc.NewValue(true)),
			fc.NewEqParamCondition(fc.NewParam("IsPrime"), fc.NewValue(false)),
		).
		NextBranch(fc.CreateSequenceDag(sieveJs)).
		NextBranch(fc.CreateSequenceDag(incPy)).
		EndChoiceAndBuild()
	fcomp.Name = fcName

	u.AssertNil(t, errDag)
	err1 := fcomp.SaveToEtcd()
	u.AssertNil(t, err1)

	// INVOKE - we call the function composition
	params := make(map[string]interface{})
	params[isPrimePy.Signature.GetInputs()[0].Name] = input

	request := fc.NewCompositionRequest(shortuuid.New(), fcomp, params)
	resultMap, err2 := fcomp.Invoke(request)
	u.AssertNil(t, err2)

	// checking the result
	output := resultMap.Result[sieveJs.Signature.GetOutputs()[1].Name]
	slice, err := u.ConvertToSlice(output)
	u.AssertNil(t, err)

	res, err := u.ConvertInterfaceToSpecificSlice[float64](slice)
	u.AssertNil(t, err)

	u.AssertSliceEqualsMsg[float64](t, []float64{2, 3, 5, 7, 11, 13}, res, "output is wrong")

	// cleaning up function composition and function
	err3 := fcomp.Delete()
	u.AssertNil(t, err3)
}

func TestInvokeCompositionError(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test")
	}
	fcName := "error"

	incPy, errDp := InitializePyFunction("inc", "handler", function.NewSignature().
		AddInput("input", function.Int{}).
		AddOutput("result", function.Int{}).Build())
	u.AssertNil(t, errDp)

	fcomp, errDag := fc.NewDagBuilder().
		AddChoiceNode(
			fc.NewEqParamCondition(fc.NewParam("NonExistentParam"), fc.NewValue(true)),
			fc.NewEqCondition(2, 3),
		).
		NextBranch(fc.CreateSequenceDag(incPy)).
		EndChoiceAndBuild()
	fcomp.Name = fcName
	u.AssertNil(t, errDag)
	err1 := fcomp.SaveToEtcd()
	u.AssertNil(t, err1)

	// INVOKE - we call the function composition
	params := make(map[string]interface{})
	params[incPy.Signature.GetInputs()[0].Name] = 1

	request := fc.NewCompositionRequest(shortuuid.New(), fcomp, params)
	_, err2 := fcomp.Invoke(request)
	u.AssertNonNil(t, err2)
}

func TestInvokeCompositionFailAndSucceed(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test")
	}

	fcomp, errDag := fc.NewDagBuilder().
		AddChoiceNode(
			fc.NewEqParamCondition(fc.NewParam("value"), fc.NewValue(1)),
			fc.NewConstCondition(true),
		).
		NextBranch(fc.NewDagBuilder().AddSucceedNodeAndBuild("everything ok")).
		NextBranch(fc.NewDagBuilder().AddFailNodeAndBuild("FakeError", "This should be an error")).
		EndChoiceAndBuild()
	u.AssertNil(t, errDag)
	fcomp.Name = "fail_succeed"
	err1 := fcomp.SaveToEtcd()
	u.AssertNil(t, err1)

	// First run: Success

	// INVOKE - we call the function composition
	params := make(map[string]interface{})
	params["value"] = 1

	request := fc.NewCompositionRequest(shortuuid.New(), fcomp, params)
	resultMap, errInvoke1 := fcomp.Invoke(request)
	u.AssertNilMsg(t, errInvoke1, "error while invoking the branch (succeed)")

	result, err := resultMap.GetIntSingleResult()
	u.AssertNilMsg(t, err, "Result not found")
	u.AssertEquals(t, 1, result)

	// Second run: Fail
	params2 := make(map[string]interface{})
	params2["value"] = 2

	request2 := fc.NewCompositionRequest(shortuuid.New(), fcomp, params2)
	resultMap2, errInvoke2 := fcomp.Invoke(request2)
	u.AssertNilMsg(t, errInvoke2, "error while invoking the branch (fail)")

	valueError, found := resultMap2.Result["FakeError"]
	u.AssertTrueMsg(t, found, "FakeError not found")
	causeStr, ok := valueError.(string)

	u.AssertTrueMsg(t, ok, "cause value is not a string")
	u.AssertEquals(t, "This should be an error", causeStr)
}

func TestInvokeCompositionPassDoNothing(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test")
	}

	incPy, errDp := InitializePyFunction("inc", "handler", function.NewSignature().
		AddInput("input", function.Int{}).
		AddOutput("result", function.Int{}).Build())
	u.AssertNil(t, errDp)
	fcomp, errDag := fc.NewDagBuilder().
		AddSimpleNode(incPy).
		AddPassNode(""). // this should not do nothing
		AddSimpleNode(incPy).
		Build()
	fcomp.Name = "pass_do_nothing"
	u.AssertNil(t, errDag)

	err1 := fcomp.SaveToEtcd()
	u.AssertNil(t, err1)

	params := make(map[string]interface{})
	params["input"] = 1

	request := fc.NewCompositionRequest(shortuuid.New(), fcomp, params)
	resultMap, errInvoke1 := fcomp.Invoke(request)
	u.AssertNilMsg(t, errInvoke1, "error while invoking the composition with pass node")

	result, err := resultMap.GetIntSingleResult()
	u.AssertNilMsg(t, err, "Result not found")
	u.AssertEquals(t, 3, result)
}

func TestInvokeCompositionWait(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test")
	}

	incPy, errDp := InitializePyFunction("inc", "handler", function.NewSignature().
		AddInput("input", function.Int{}).
		AddOutput("result", function.Int{}).Build())
	u.AssertNil(t, errDp)
	fcomp, errDag := fc.NewDagBuilder().
		AddSimpleNode(incPy).
		AddWaitNode(2). // this should not do nothing
		AddSimpleNode(incPy).
		Build()
	u.AssertNil(t, errDag)

	fcomp.Name = "pass_do_nothing"
	err1 := fcomp.SaveToEtcd()
	u.AssertNil(t, err1)

	params := make(map[string]interface{})
	params["input"] = 1

	request := fc.NewCompositionRequest(shortuuid.New(), fcomp, params)
	resultMap, errInvoke1 := fcomp.Invoke(request)
	u.AssertNilMsg(t, errInvoke1, "error while invoking the composition with pass node")

	result, err := resultMap.GetIntSingleResult()
	u.AssertNilMsg(t, err, "Result not found")
	u.AssertEquals(t, 3, result)

	// find wait node
	var waitNode *fc.WaitNode = nil
	ok := false
	for _, nodeInDag := range fcomp.Nodes {
		waitNode, ok = nodeInDag.(*fc.WaitNode)
		if ok {
			break
		}
	}
	u.AssertTrueMsg(t, ok, "failed to find wait node")

	respTime, ok := resultMap.Reports.Get(fc.CreateExecutionReportId(waitNode))
	u.AssertTrueMsg(t, ok, "failed to find execution report for wait node")
	u.AssertTrueMsg(t, respTime.Duration > 2.0, fmt.Sprintf("wait node has waited the wrong amount of time %f, expected at least 2.0 seconds", respTime.Duration))
}
