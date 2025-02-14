package test

import (
	"github.com/serverledge-faas/serverledge/internal/function"
	"os"
	"testing"

	"github.com/lithammer/shortuuid"
	"github.com/serverledge-faas/serverledge/internal/workflow"
	"github.com/serverledge-faas/serverledge/utils"
)

// / TestParsedWorkflowName verifies that the composition name matches the filename (without extension)
func TestParsedWorkflowName(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test")
	}

	initializeAllPyFunctionFromNames(t, "inc", "double", "hello")

	expectedName := "simple"
	comp := parseFileName(t, expectedName)
	// the name should be simple, because we parsed the "simple.json" file
	utils.AssertEquals(t, comp.Name, expectedName)

	// delete each function
	deleteApiTest(t, "inc", HOST, PORT)
	deleteApiTest(t, "double", HOST, PORT)
	deleteApiTest(t, "hello", HOST, PORT)
}

// commonTest creates a function, parses a json AWS State Language file producing a function composition,
// then checks if the composition is saved onto ETCD. Lastly, it runs the composition and expects the correct result.
func commonTest(t *testing.T, name string, expectedResult int) {
	all, err := workflow.GetAllWorkflows()
	utils.AssertNil(t, err)

	//initializeAllPyFunctionFromNames(t, "inc", "double", "hello", "noop")

	comp := parseFileName(t, name)
	defer func() {
		err = comp.Delete()
		utils.AssertNilMsg(t, err, "failed to delete composition")
	}()
	// saving to etcd is not necessary to run the function composition, but is needed when offloading
	{
		err := comp.Save()
		utils.AssertNilMsg(t, err, "unable to save parsed composition")

		all2, err := workflow.GetAllWorkflows()
		utils.AssertNil(t, err)
		utils.AssertEqualsMsg(t, len(all2), len(all)+1, "the number of created functions differs")

		expectedComp, ok := workflow.Get(name)
		utils.AssertTrue(t, ok)

		utils.AssertTrueMsg(t, comp.Equals(expectedComp), "parsed composition differs from expected composition")
	}

	// runs the workflow
	params := make(map[string]interface{})
	params["input"] = "0"
	request := workflow.NewRequest(shortuuid.New(), comp, params)
	_, err2 := comp.Invoke(request)
	utils.AssertNil(t, err2)
}

// TestParsingSimple verifies that a simple json with 2 state is correctly parsed and it is equal to a sequence workflow with 2 simple nodes

func TestParsingSimple(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test")
	}

	initializeAllPyFunctionFromNames(t, "hello")
	commonTest(t, "simple", 2)
	deleteApiTest(t, "hello", HOST, PORT)
}

// TestParsingSequence verifies that a json with 5 simple nodes is correctly parsed
func TestParsingSequence(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test")
	}

	InitializePyFunction("noop", "handler", function.NewSignature().Build())

	commonTest(t, "sequence", 5)
	deleteApiTest(t, "noop", HOST, PORT)

}

// TestParsingMixedUpSequence verifies that a json file with 5 simple unordered task is parsed correctly and in order in a sequence workflow.
func TestParsingMixedUpSequence(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test")
	}

	initializeAllPyFunctionFromNames(t, "double", "inc")
	commonTest(t, "mixed_sequence", 5)
	deleteApiTest(t, "double", HOST, PORT)
	deleteApiTest(t, "inc", HOST, PORT)
}

// / TestParsingChoiceWorkflowWithDefaultFail verifies that a json file with three different choices is correctly parsed in a Workflow with a Choice node and three simple nodes.
func TestParsingChoiceWorkflowWithDefaultFail(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test")
	}

	// Creates "inc", "double" and "hello" python functions
	fns := initializeAllPyFunctionFromNames(t, "inc", "double", "hello")

	incFn := fns[0]

	// reads the file
	body, err := os.ReadFile("asl/choice_numeq_succeed_fail.json")
	utils.AssertNilMsg(t, err, "unable to read file")
	// parse the ASL language
	comp, err := workflow.FromASL("choice", body)
	utils.AssertNilMsg(t, err, "unable to parse json")

	// runs the workflow, making it going to the fail part
	params := make(map[string]interface{})
	params[incFn.Signature.GetInputs()[0].Name] = 10
	request := workflow.NewRequest(shortuuid.New(), comp, params)
	resultMap, err2 := comp.Invoke(request)
	utils.AssertNil(t, err2)

	expectedKey := "DefaultStateError"
	expectedValue := "No Matches!"

	// There should be the error/cause pair, and only that
	value, keyExist := resultMap.Result[expectedKey]
	valueStr, isString := value.(string)
	utils.AssertTrueMsg(t, keyExist, "key "+expectedKey+"does not exist")
	utils.AssertTrueMsg(t, isString, "value is not a string")
	utils.AssertEqualsMsg(t, len(resultMap.Result), 1, "there is not exactly one result")
	utils.AssertEqualsMsg(t, expectedValue, valueStr, "values don't match")

	deleteApiTest(t, "inc", HOST, PORT)
	deleteApiTest(t, "hello", HOST, PORT)
	deleteApiTest(t, "double", HOST, PORT)
}

// 1st branch (input==1): inc + inc (expected nothing)
// 2nd branch (input==2): double + inc (expected nothing)
// def branch (true    ): hello (expected nothing)
func TestParsingChoiceWorkflowWithDataTestExpr(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test")
	}
	// Creates "inc", "double" and "hello" python functions
	funcs := initializeAllPyFunctionFromNames(t, "inc", "double", "hello")

	// reads the file
	body, err := os.ReadFile("asl/choice_datatestexpr.json")
	utils.AssertNilMsg(t, err, "unable to read file")
	// parse the ASL language
	comp, err := workflow.FromASL("choice3", body)
	utils.AssertNilMsg(t, err, "unable to parse json")

	incFn := funcs[0]
	// helloFn := funcs[len(funcs)-1]

	// runs the workflow (1st choice branch) test: (input == 1)
	params1 := make(map[string]interface{})
	params1[incFn.Signature.GetInputs()[0].Name] = 1
	request1 := workflow.NewRequest(shortuuid.New(), comp, params1)
	resultMap1, err1 := comp.Invoke(request1)
	utils.AssertNil(t, err1)

	// checks that output is (1+1)*2=4
	output := resultMap1.Result[incFn.Signature.GetOutputs()[0].Name]
	utils.AssertEquals(t, 4, output.(int))
	// runs the workflow (2nd choice branch) test: (input == 2)
	params2 := make(map[string]interface{})
	params2[incFn.Signature.GetInputs()[0].Name] = 2
	request2 := workflow.NewRequest(shortuuid.New(), comp, params2)
	resultMap, err2 := comp.Invoke(request2)
	utils.AssertNil(t, err2)

	// check that output is 2*2*2 = 8
	output2 := resultMap.Result[incFn.Signature.GetOutputs()[0].Name]
	utils.AssertEquals(t, 8, output2.(int))

	// runs the workflow (default choice branch)
	paramsDefault := make(map[string]interface{})
	paramsDefault[incFn.Signature.GetInputs()[0].Name] = "Giacomo"
	requestDefault := workflow.NewRequest(shortuuid.New(), comp, paramsDefault)
	resultMap, errDef := comp.Invoke(requestDefault)
	utils.AssertNil(t, errDef)

	deleteApiTest(t, "inc", HOST, PORT)
	deleteApiTest(t, "hello", HOST, PORT)
	deleteApiTest(t, "double", HOST, PORT)
}

func TestParsingChoiceWorkflowWithBoolExpr(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test")
	}

	// Creates "inc", "double" and "hello" python functions
	_ = initializeAllPyFunctionFromNames(t, "inc", "double", "hello")

	// reads the file
	body, err := os.ReadFile("asl/choice_boolexpr.json")
	utils.AssertNilMsg(t, err, "unable to read file")
	// parse the ASL language
	comp, err := workflow.FromASL("choice2", body)
	utils.AssertNilMsg(t, err, "unable to parse json")

	// 1st branch (type != "Private")
	params := make(map[string]interface{})
	params["type"] = "Public"
	params["value"] = 1
	//params["input"] = 1
	request := workflow.NewRequest(shortuuid.New(), comp, params)
	resultMap, err1 := comp.Invoke(request)
	utils.AssertNil(t, err1)

	// checks the result (1+1+1 = 3)
	output, err := GetIntSingleResult(&resultMap)
	utils.AssertNilMsg(t, err, "failed to get int single result")
	utils.AssertEquals(t, 3, output)

	// 2nd branch (type == "Private", value is present, value is numeric, value >= 20, value < 30)
	params2 := make(map[string]interface{})
	params2["type"] = "Private"
	params2["value"] = 20
	request2 := workflow.NewRequest(shortuuid.New(), comp, params2)
	resultMap2, err2 := comp.Invoke(request2)
	utils.AssertNil(t, err2)

	// checks the result (20*2+1 = 41)
	output2, err := GetIntSingleResult(&resultMap2)
	utils.AssertNilMsg(t, err, "failed to get int single result")
	utils.AssertEquals(t, 41, output2)

	// 2nd branch (type == "Private", value is present, value is numeric, value >= 20, value < 30)
	params3 := make(map[string]interface{})
	params3["type"] = "Private"
	request3 := workflow.NewRequest(shortuuid.New(), comp, params3)
	comp.Invoke(request3)
	utils.AssertNil(t, err2)
	// no results to check

	deleteApiTest(t, "inc", HOST, PORT)
	deleteApiTest(t, "hello", HOST, PORT)
	deleteApiTest(t, "double", HOST, PORT)
}

func TestParsingWorkflowWithMalformedJson(t *testing.T) {}

func TestParsingWorkflowWithUnknownFunction(t *testing.T) {}
