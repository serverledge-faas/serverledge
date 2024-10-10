package test

/// fc_test contains test that executes serverledge server-side function composition apis directly. Internally it uses __function__ REST API.
import (
	"encoding/json"
	"fmt"
	"log"
	"testing"

	"github.com/cornelk/hashmap"
	"github.com/grussorusso/serverledge/internal/fc"
	"github.com/grussorusso/serverledge/internal/function"
	"github.com/grussorusso/serverledge/internal/node"
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
	dag, err := fc.CreateSequenceDag(fn, fn, fn)
	u.AssertNil(t, err)
	composition, err := fc.NewFC(fcName, *dag, []*function.Function{fn}, true)
	u.AssertNil(t, err)

	marshaledFunc, errMarshal := json.Marshal(composition)
	u.AssertNilMsg(t, errMarshal, "failed to marshal composition")
	var retrieved fc.FunctionComposition
	errUnmarshal := json.Unmarshal(marshaledFunc, &retrieved)
	u.AssertNilMsg(t, errUnmarshal, "failed composition unmarshal")

	u.AssertTrueMsg(t, retrieved.Equals(composition), fmt.Sprintf("retrieved composition is not equal to initial composition. Retrieved : %s, Expected %s ", retrieved.String(), composition.String()))
}

func TestUnmarshalFunctionCompositionResult(t *testing.T) {
	// composition := "{\n\t\"Reports\": {\n\t\t\"End_9TUZZdXNwgroNYp4akDKQ6\": {\n\t\t\t\"Result\": \"end\",\n\t\t\t\"ResponseTime\": 0,\n\t\t\t\"IsWarmStart\": false,\n\t\t\t\"InitTime\": 0,\n\t\t\t\"OffloadLatency\": 0,\n\t\t\t\"Duration\": 0,\n\t\t\t\"SchedAction\": \"\"\n\t\t},\n\t\t\"Simple_JyzhDkLuBzUVSmPEUiEWVm\": {\n\t\t\t\"Result\": \"3\",\n\t\t\t\"ResponseTime\": 0.00283594,\n\t\t\t\"IsWarmStart\": true,\n\t\t\t\"InitTime\": 0.000029114,\n\t\t\t\"OffloadLatency\": 0,\n\t\t\t\"Duration\": 0.002802751,\n\t\t\t\"SchedAction\": \"\"\n\t\t},\n\t\t\"Simple_c7A3CSJ9efgnW2uCvgWt3Y\": {\n\t\t\t\"Result\": \"4\",\n\t\t\t\"ResponseTime\": 0.002977264,\n\t\t\t\"IsWarmStart\": true,\n\t\t\t\"InitTime\": 0.000020023,\n\t\t\t\"OffloadLatency\": 0,\n\t\t\t\"Duration\": 0.002953664,\n\t\t\t\"SchedAction\": \"\"\n\t\t},\n\t\t\"Simple_z4Jp4LXWFoPnEFFNhJQ64j\": {\n\t\t\t\"Result\": \"2\",\n\t\t\t\"ResponseTime\": 15.901950313,\n\t\t\t\"IsWarmStart\": false,\n\t\t\t\"InitTime\": 12.705640725,\n\t\t\t\"OffloadLatency\": 0,\n\t\t\t\"Duration\": 3.196273017,\n\t\t\t\"SchedAction\": \"\"\n\t\t},\n\t\t\"Start_wxrH86t6zc2T2menLrUgYm\": {\n\t\t\t\"Result\": \"start\",\n\t\t\t\"ResponseTime\": 0,\n\t\t\t\"IsWarmStart\": false,\n\t\t\t\"InitTime\": 0,\n\t\t\t\"OffloadLatency\": 0,\n\t\t\t\"Duration\": 0,\n\t\t\t\"SchedAction\": \"\"\n\t\t}\n\t},\n\t\"ResponseTime\": 0,\n\t\"Result\": {\n\t\t\"result\": 4\n\t}\n}"

	resultMap := make(map[string]interface{})
	resultMap["result"] = 4.

	reportsMap := hashmap.New[fc.ExecutionReportId, *function.ExecutionReport]()
	reportsMap.Set("Simple_JyzhDkLuBzUVSmPEUiEWVm", &function.ExecutionReport{ResponseTime: 0.00283594, IsWarmStart: true, InitTime: 0.000029114, OffloadLatency: 0.000000, Duration: 0.002802751, SchedAction: "", Output: "", Result: "3"})
	reportsMap.Set("Simple_c7A3CSJ9efgnW2uCvgWt3Y", &function.ExecutionReport{ResponseTime: 0.002977264, IsWarmStart: true, InitTime: 0.000020023, OffloadLatency: 0.000000, Duration: 0.002953664, SchedAction: "", Output: "", Result: "4"})
	reportsMap.Set("End_9TUZZdXNwgroNYp4akDKQ6", &function.ExecutionReport{ResponseTime: 0.000000, IsWarmStart: false, InitTime: 0.000000, OffloadLatency: 0.000000, Duration: 0.000000, SchedAction: "", Output: "", Result: "end"})
	reportsMap.Set("Start_wxrH86t6zc2T2menLrUgYm", &function.ExecutionReport{ResponseTime: 0.000000, IsWarmStart: false, InitTime: 0.000000, OffloadLatency: 0.000000, Duration: 0.000000, SchedAction: "", Output: "", Result: "start"})
	reportsMap.Set("Simple_z4Jp4LXWFoPnEFFNhJQ64j", &function.ExecutionReport{ResponseTime: 15.901950313, IsWarmStart: false, InitTime: 12.705640725, OffloadLatency: 0.000000, Duration: 3.196273017, SchedAction: "", Output: "", Result: "2"})

	expected := &fc.CompositionExecutionReport{
		Result:       resultMap,
		Reports:      reportsMap,
		ResponseTime: 0.000000,
		// Progress is not checked
	}

	marshal, errMarshal := json.Marshal(expected)
	u.AssertNil(t, errMarshal)

	var retrieved fc.CompositionExecutionReport
	errUnmarshal := json.Unmarshal(marshal, &retrieved)

	u.AssertNilMsg(t, errUnmarshal, "failed to unmarshal composition result")
	u.AssertNonNilMsg(t, retrieved.Result, "the unmarshalled composition result should not have been nil")
	u.AssertNonNilMsg(t, retrieved.Reports, "the unmarshalled composition result should not have been nil")

	u.AssertTrueMsg(t, retrieved.Equals(expected), fmt.Sprintf("execution report differs first: %v\n second: %v", retrieved, expected))
}

// TestComposeFC checks the CREATE, GET and DELETE functionality of the Function Composition
func TestComposeFC(t *testing.T) {

	if testing.Short() {
		t.Skip("Skipping integration test")
	}

	// GET1 - initially we do not have any function composition
	funcs, err := fc.GetAllFC()
	fmt.Println(funcs)
	lenFuncs := len(funcs)
	u.AssertNil(t, err)
	u.AssertEqualsMsg(t, 0, lenFuncs, "There are more than 0 registered function compositions. Maybe some other test has failed")

	fcName := "test"
	// CREATE - we create a test function composition
	m := make(map[string]interface{})
	m["input"] = 0
	length := 3
	_, fArr, err := initializeSameFunctionSlice(length, "js")
	u.AssertNil(t, err)

	dag, err := fc.CreateSequenceDag(fArr...)
	u.AssertNil(t, err)

	fcomp, err := fc.NewFC(fcName, *dag, fArr, true)
	u.AssertNil(t, err)
	err2 := fcomp.SaveToEtcd()

	u.AssertNil(t, err2)

	// The creation is successful: we have one more function composition?
	// GET2
	funcs2, err3 := fc.GetAllFC()
	fmt.Println(funcs2)
	u.AssertNil(t, err3)
	u.AssertEqualsMsg(t, lenFuncs+1, len(funcs2), "creation of function failed")

	// the function is exactly the one i created?
	fun, ok := fc.GetFC(fcName)
	u.AssertTrue(t, ok)
	u.AssertTrue(t, fcomp.Equals(fun))

	// DELETE
	err4 := fcomp.Delete()
	u.AssertNil(t, err4)

	// The deletion is successful?
	// GET3
	funcs3, err5 := fc.GetAllFC()
	fmt.Println(funcs3)
	u.AssertNil(t, err5)
	u.AssertEqualsMsg(t, len(funcs3), lenFuncs, "deletion of function failed")
}

// TestInvokeFC executes a Sequential Dag of length N, where each node executes a simple increment function.
func TestInvokeFC(t *testing.T) {

	if testing.Short() {
		t.Skip("Skipping integration test")
	}

	fcName := "test"
	// CREATE - we create a test function composition
	length := 5
	f, fArr, err := initializeSameFunctionSlice(length, "js")
	u.AssertNil(t, err)
	dag, errDag := fc.CreateSequenceDag(fArr...)
	u.AssertNil(t, errDag)
	fcomp, err := fc.NewFC(fcName, *dag, fArr, true)
	u.AssertNil(t, err)
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
	// res, errConv := strconv.Atoi(output.(string))
	u.AssertEquals(t, length, output.(int))
	// u.AssertNil(t, errConv)
	fmt.Printf("%+v\n", resultMap)

	// cleaning up function composition and function
	err3 := fcomp.Delete()
	u.AssertNil(t, err3)

	//u.AssertTrueMsg(t, fc.IsEmptyPartialDataCache(), "partial data cache is not empty")
}

// TestInvokeChoiceFC executes a Choice Dag with N alternatives, and it executes only the second one. The functions are all the same increment function
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

	dag, errDag := fc.NewDagBuilder().
		AddChoiceNode(
			fc.NewConstCondition(false),
			fc.NewSmallerCondition(2, 1),
			fc.NewConstCondition(true),
		).
		NextBranch(fc.CreateSequenceDag(incJs)).
		NextBranch(fc.CreateSequenceDag(incPy)).
		NextBranch(fc.CreateSequenceDag(doublePy)).
		EndChoiceAndBuild()

	u.AssertNil(t, errDag)
	fcomp, err := fc.NewFC(fcName, *dag, []*function.Function{incJs, incPy, doublePy}, true)
	u.AssertNil(t, err)
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
	fmt.Printf("%s\n", resultMap.String())

	// cleaning up function composition and function
	err3 := fcomp.Delete()
	u.AssertNil(t, err3)
}

// TestInvokeFC_DifferentFunctions executes a Sequential Dag of length 2, with two different functions (in different languages)
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

	dag, errDag := fc.NewDagBuilder().
		AddSimpleNode(fDouble).
		AddSimpleNode(fInc).
		AddSimpleNode(fDouble).
		AddSimpleNode(fInc).
		Build()

	u.AssertNil(t, errDag)

	fcomp, err := fc.NewFC(fcName, *dag, []*function.Function{fDouble, fInc}, true)
	u.AssertNil(t, err)
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

	// res, errConv := strconv.Atoi(output.(string))
	u.AssertEquals(t, (2*2+1)*2+1, output.(int))
	// u.AssertNil(t, errConv)
	fmt.Println(resultMap)

	// cleaning up function composition and function
	err3 := fcomp.Delete()
	u.AssertNil(t, err3)

	//u.AssertTrueMsg(t, fc.IsEmptyPartialDataCache(), "partial data cache is not empty")
}

// TestInvokeFC_BroadcastFanOut executes a Parallel Dag with N parallel branches
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
	dag, errDag := fc.CreateBroadcastDag(func() (*fc.Dag, error) { return fc.CreateSequenceDag(fDouble) }, width)
	u.AssertNil(t, errDag)
	dag.Print()

	fcomp, err := fc.NewFC(fcName, *dag, []*function.Function{fDouble}, true)
	u.AssertNil(t, err)
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

// TestInvokeFC_Concurrent executes concurrently m times a Sequential Dag of length N, where each node executes a simple increment function.
func TestInvokeFC_Concurrent(t *testing.T) {

	if testing.Short() {
		t.Skip("Skipping integration test")
	}

	fcName := "test"
	// CREATE - we create a test function composition
	length := 5
	f, fArr, err := initializeSameFunctionSlice(length, "py")
	u.AssertNil(t, err)
	builder := fc.NewDagBuilder()
	for i := 0; i < length; i++ {
		builder.AddSimpleNodeWithId(f, fmt.Sprintf("simple %d", i))
	}
	dag, errDag := builder.Build()
	u.AssertNil(t, errDag)

	fcomp, err := fc.NewFC(fcName, *dag, fArr, true)
	u.AssertNil(t, err)
	err1 := fcomp.SaveToEtcd()
	u.AssertNil(t, err1)

	concurrencyLevel := 10
	start := make(chan int)
	results := make(map[int]chan interface{})
	errors := make(map[int]chan error)
	// initialize channels
	for i := 0; i < concurrencyLevel; i++ {
		results[i] = make(chan interface{})
		errors[i] = make(chan error)
	}

	fmt.Println("initializing goroutines...")
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
			fmt.Printf("goroutine %d started invoking\n", i)
			// return error
			resultMap, err2 := fcomp.Invoke(request)
			errChan <- err2
			// return result
			output := resultMap.Result[f.Signature.GetOutputs()[0].Name]
			fmt.Printf("goroutine %d - result: %d\n", i, resultMap.Result["result"])
			resultChan <- output
		}(i, resultChan, errChan, start)
	}
	// let's start all the goroutines at the same time
	for i := 0; i < concurrencyLevel; i++ {
		start <- 1
	}

	// and wait for errors (hopefully not) and results
	for i, e := range errors {
		fmt.Printf("waiting for errors for goroutine %d...\n", i)
		maybeError := <-e
		u.AssertNilMsg(t, maybeError, "error in goroutine")
	}

	for i, r := range results {
		fmt.Printf("waiting for result for goroutine %d...\n", i)
		output := <-r
		fmt.Printf("result of goroutine %d = %d\n", i, output.(int))
		u.AssertEqualsMsg(t, length+i, output.(int), fmt.Sprintf("output of goroutine %d is wrong", i))
	}

	fmt.Println("deleting all composition and functions...")
	// cleaning up function composition and function
	err3 := fcomp.Delete()
	u.AssertNil(t, err3)

	// removing functions container to release resources
	for _, fun := range fcomp.Functions {
		// Delete local warm containers
		node.ShutdownWarmContainersFor(fun)
	}
}

// TestInvokeFC_Complex_Concurrent executes concurrently m times a complex Dag of length N, where each node executes a different function
func TestInvokeFC_Complex_Concurrent(t *testing.T) {

	if testing.Short() {
		t.Skip("Skipping integration test")
	}

	// CREATE - we create a test function composition
	fcomp, err := createComplexComposition(t)
	u.AssertNil(t, err)

	concurrencyLevel := 6
	start := make(chan int)
	results := make(map[int]chan interface{})
	errors := make(map[int]chan error)
	// initialize channels
	for i := 0; i < concurrencyLevel; i++ {
		results[i] = make(chan interface{})
		errors[i] = make(chan error)
	}

	fmt.Println("initializing goroutines...")
	for i := 0; i < concurrencyLevel; i++ {
		resultChan := results[i]
		errChan := errors[i]
		// INVOKE - we call the function composition concurrently m times
		go func(i int, resultChan chan interface{}, errChan chan error, start chan int) {
			params := make(map[string]interface{})
			goName := ""
			outName := ""
			if i%3 == 0 { // word_count
				params["InputText"] = "Word counting is a useful technique for analyzing text data. It helps in various natural language processing tasks. In this example, we are testing the wordCount function in JavaScript. It should accurately count the number of words in this text. Counting words can be a fundamental step in text analysis."
				params["Task"] = true
				goName = "word_count"
				outName = "NumberOfWords"
			} else if i%3 == 1 { // summarize
				params["InputText"] = "The Solar System consists of the Sun and all the celestial objects bound to it by gravity, including the eight major planets and their moons, asteroids, comets, and more. The Sun is a star located at the center of the Solar System. It provides light, heat, and energy, making life possible on Earth.\n\nThe eight major planets in our Solar System are Mercury, Venus, Earth, Mars, Jupiter, Saturn, Uranus, and Neptune. Each planet has unique characteristics, and some have moons of their own. For example, Earth has one natural satellite, the Moon.\n\nAsteroids are rocky objects that orbit the Sun, mainly found in the asteroid belt between the orbits of Mars and Jupiter. Comets are icy bodies that develop tails when they approach the Sun.\n\nStudying the Solar System provides insights into the formation and evolution of celestial bodies, as well as the potential for extraterrestrial life. Scientists use various tools and telescopes to explore and learn more about the mysteries of our Solar System.\n"
				params["Task"] = false
				goName = "summarize"
				outName = "Summary"
			} else { // 2 parallel grep
				params["InputText"] = []string{
					"This is an example text for testing the grep function.\nYou can use the grep function to search for specific words or patterns in text.\nThe function is a powerful tool for text processing.\n",
					"It allows you to filter and extract lines that match a given pattern.\nYou can customize the pattern using regular expressions.\nFeel free to test the grep function with different patterns and texts.",
				}
				goName = "grep"
				outName = "Rows"
			}

			request := fc.NewCompositionRequest(fmt.Sprintf("goroutine_%d_branch_%s", i, goName), fcomp, params)
			// wait until all goroutines are ready
			<-start
			fmt.Printf("goroutine %d started invoking\n", i)
			// return error
			resultMap, err2 := fcomp.Invoke(request)
			errChan <- err2
			// return result
			output := resultMap.Result[outName]
			fmt.Printf("goroutine %d branch %s - result %s: %v\n", i, goName, outName, output)
			resultChan <- output
		}(i, resultChan, errChan, start)
	}
	// let's start all the goroutines at the same time
	for i := 0; i < concurrencyLevel; i++ {
		start <- 1
	}

	// and wait for errors (hopefully not) and results
	for i, e := range errors {
		fmt.Printf("waiting for errors for goroutine %d...\n", i)
		maybeError := <-e
		u.AssertNilMsg(t, maybeError, "error in goroutine")
	}

	for i, r := range results {
		fmt.Printf("waiting for result for goroutine %d...\n", i)
		output := <-r
		fmt.Printf("result of goroutine %d = %v\n", i, output)
	}

	fmt.Println("deleting all composition and functions...")
	// cleaning up function composition and function
	err3 := fcomp.Delete()
	u.AssertNil(t, err3)

	// removing functions container to release resources
	for _, fun := range fcomp.Functions {
		// Delete local warm containers
		node.ShutdownWarmContainersFor(fun)
	}
}

// TestInvokeFC_DifferentBranches executes a Parallel broadcast Dag with N parallel DIFFERENT branches.
func TestInvokeFC_DifferentBranches(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test")
	}
	//for i := 0; i < 1; i++ {

	fcName := "test"
	// CREATE - we create a test function composition
	f, errF1 := InitializePyFunction("double", "handler", function.NewSignature().
		AddInput("input", function.Int{}).
		AddOutput("result", function.Int{}).
		Build())
	u.AssertNil(t, errF1)

	dag, errDag := fc.CreateBroadcastMultiFunctionDag(
		func() (*fc.Dag, error) { return fc.CreateSequenceDag(f) },
		func() (*fc.Dag, error) { return fc.CreateSequenceDag(f, f) },
		func() (*fc.Dag, error) { return fc.CreateSequenceDag(f, f, f) },
	)
	u.AssertNil(t, errDag)
	dag.Print()

	fcomp, err := fc.NewFC(fcName, *dag, []*function.Function{f}, true)
	u.AssertNil(t, err)
	err1 := fcomp.SaveToEtcd()
	u.AssertNil(t, err1)

	// INVOKE - we call the function composition
	params := make(map[string]interface{})
	params[f.Signature.GetInputs()[0].Name] = 1
	request := fc.NewCompositionRequest(shortuuid.New(), fcomp, params)
	resultMap, err2 := fcomp.Invoke(request)
	u.AssertNil(t, err2) // we should check that is a timeout error

	output := resultMap.Result
	u.AssertNonNil(t, output)

	expectedMap := make(map[string]int)
	expectedMap["result"] = 2
	expectedMap["result_1"] = 4
	expectedMap["result_2"] = 8

	u.AssertMapEquals[string, int](t, expectedMap, output)

	// cleaning up function composition and functions
	err3 := fcomp.Delete()
	u.AssertNil(t, err3)
}

// TestInvokeFC_ScatterFanOut executes a Parallel Dag with N parallel branches
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
	dag, errDag := fc.CreateScatterSingleFunctionDag(fDouble, width)
	u.AssertNil(t, errDag)
	dag.Print()

	fcomp, err := fc.NewFC(fcName, *dag, []*function.Function{fDouble}, true)
	u.AssertNil(t, err)
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
	for key, res := range output {
		fmt.Printf("%s : %v\n", key, res)
		genericSlice, ok := res.([]interface{})
		u.AssertTrue(t, ok)
		specificSlice, err := u.ConvertToSpecificSlice[int](genericSlice)
		u.AssertNil(t, err)
		u.AssertSliceEquals[int](t, []int{2, 4, 6}, specificSlice)
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

	dag, errDag := fc.NewDagBuilder().
		AddSimpleNode(isPrimePy).
		AddChoiceNode(
			fc.NewEqParamCondition(fc.NewParam("IsPrime"), fc.NewValue(true)),
			fc.NewEqParamCondition(fc.NewParam("IsPrime"), fc.NewValue(false)),
		).
		NextBranch(fc.CreateSequenceDag(sieveJs)).
		NextBranch(fc.CreateSequenceDag(incPy)).
		EndChoiceAndBuild()

	u.AssertNil(t, errDag)
	fcomp, err := fc.NewFC(fcName, *dag, []*function.Function{isPrimePy, sieveJs, incPy}, true)
	u.AssertNil(t, err)
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
	fmt.Printf("%+v\n", resultMap)

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

	dag, errDag := fc.NewDagBuilder().
		AddChoiceNode(
			fc.NewEqParamCondition(fc.NewParam("NonExistentParam"), fc.NewValue(true)),
			fc.NewEqCondition(2, 3),
		).
		NextBranch(fc.CreateSequenceDag(incPy)).
		EndChoiceAndBuild()
	u.AssertNil(t, errDag)
	fcomp, err := fc.NewFC(fcName, *dag, []*function.Function{incPy}, true)
	u.AssertNil(t, err)
	err1 := fcomp.SaveToEtcd()
	u.AssertNil(t, err1)

	// INVOKE - we call the function composition
	params := make(map[string]interface{})
	params[incPy.Signature.GetInputs()[0].Name] = 1

	request := fc.NewCompositionRequest(shortuuid.New(), fcomp, params)
	_, err2 := fcomp.Invoke(request)
	u.AssertNonNil(t, err2)

	request.ExecReport.Progress.Print()
}

func TestInvokeCompositionFailAndSucceed(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test")
	}

	dag, errDag := fc.NewDagBuilder().
		AddChoiceNode(
			fc.NewEqParamCondition(fc.NewParam("value"), fc.NewValue(1)),
			fc.NewConstCondition(true),
		).
		NextBranch(fc.NewDagBuilder().AddSucceedNodeAndBuild("everything ok")).
		NextBranch(fc.NewDagBuilder().AddFailNodeAndBuild("FakeError", "This should be an error")).
		EndChoiceAndBuild()
	u.AssertNil(t, errDag)
	fcomp, err := fc.NewFC("fail_succeed", *dag, []*function.Function{}, true)
	u.AssertNil(t, err)
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
	dag, errDag := fc.NewDagBuilder().
		AddSimpleNode(incPy).
		AddPassNode(""). // this should not do nothing
		AddSimpleNode(incPy).
		Build()
	u.AssertNil(t, errDag)

	fcomp, err := fc.NewFC("pass_do_nothing", *dag, []*function.Function{incPy}, true)
	u.AssertNil(t, err)
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
	dag, errDag := fc.NewDagBuilder().
		AddSimpleNode(incPy).
		AddWaitNode(2). // this should not do nothing
		AddSimpleNode(incPy).
		Build()
	u.AssertNil(t, errDag)

	fcomp, err := fc.NewFC("pass_do_nothing", *dag, []*function.Function{incPy}, true)
	u.AssertNil(t, err)
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
	for _, nodeInDag := range dag.Nodes {
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
