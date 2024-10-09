package test

import (
	"fmt"
	"os"
	"testing"

	"github.com/grussorusso/serverledge/internal/fc"
	"github.com/grussorusso/serverledge/utils"
	"github.com/lithammer/shortuuid"
)

// / TestParsedCompositionName verifies that the composition name matches the filename (without extension)
func TestParsedCompositionName(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test")
	}
	expectedName := "simple"
	comp, _ := parseFileName(t, false, expectedName)
	// the name should be simple, because we parsed the "simple.json" file
	utils.AssertEquals(t, comp.Name, expectedName)
}

// commonTest creates a function, parses a json AWS State Language file producing a function composition,
// then checks if the composition is saved onto ETCD. Lastly, it runs the composition and expects the correct result.
func commonTest(t *testing.T, name string, expectedResult int) {
	all, err := fc.GetAllFC()
	utils.AssertNil(t, err)

	comp, f := parseFileName(t, false, name)
	defer func() {
		err = comp.Delete()
		utils.AssertNilMsg(t, err, "failed to delete composition")
	}()
	// saving to etcd is not necessary to run the function composition, but is needed when offloading
	{
		err := comp.SaveToEtcd()
		utils.AssertNilMsg(t, err, "unable to save parsed composition")

		all2, err := fc.GetAllFC()
		utils.AssertNil(t, err)
		utils.AssertEqualsMsg(t, len(all2), len(all)+1, "the number of created functions differs")

		expectedComp, ok := fc.GetFC(name)
		utils.AssertTrue(t, ok)

		utils.AssertTrueMsg(t, comp.Equals(expectedComp), "parsed composition differs from expected composition")
		fmt.Println(comp)
	}

	// runs the workflow
	params := make(map[string]interface{})
	params[f.Signature.GetInputs()[0].Name] = 0
	request := fc.NewCompositionRequest(shortuuid.New(), comp, params)
	resultMap, err2 := comp.Invoke(request)
	utils.AssertNil(t, err2)

	// checks the result
	output, err := resultMap.GetIntSingleResult()
	utils.AssertNilMsg(t, err, "failed to get single int result for sequence test")
	utils.AssertEquals(t, expectedResult, output)
	fmt.Println("Result: ", output)
}

// TestParsingSimple verifies that a simple json with 2 state is correctly parsed and it is equal to a sequence dag with 2 simple nodes

func TestParsingSimple(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test")
	}

	commonTest(t, "simple", 2)
}

// TestParsingSequence verifies that a json with 5 simple nodes is correctly parsed
func TestParsingSequence(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test")
	}

	commonTest(t, "sequence", 5)

}

// TestParsingMixedUpSequence verifies that a json file with 5 simple unordered task is parsed correctly and in order in a sequence DAG.
func TestParsingMixedUpSequence(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test")
	}

	commonTest(t, "mixed_sequence", 5)
}

// / TestParsingMultipleFunctionSequence verifies that a json file with three different functions is correctly parsed from a DAG.
func TestParsingMultipleFunctionSequence(t *testing.T) {

	// TODO write json file with multiple functions
	//all, err := fc.GetAllFC()
	//utils.AssertNil(t, err)
	//
	//comp, f := parseFileName(t, name)
	//defer func() {
	//	err = comp.Delete()
	//	utils.AssertNilMsg(t, err, "failed to delete composition")
	//}()
	//// saving to etcd is not necessary to run the function composition, but is needed when offloading
	//{
	//	err := comp.SaveToEtcd()
	//	utils.AssertNilMsg(t, err, "unable to save parsed composition")
	//
	//	all2, err := fc.GetAllFC()
	//	utils.AssertNil(t, err)
	//	utils.AssertEqualsMsg(t, len(all2), len(all)+1, "the number of created functions differs")
	//
	//	expectedComp, ok := fc.GetFC(name)
	//	utils.AssertTrue(t, ok)
	//
	//	utils.AssertTrueMsg(t, comp.Equals(expectedComp), "parsed composition differs from expected composition")
	//	fmt.Println(comp)
	//}
	//
	//// runs the workflow
	//params := make(map[string]interface{})
	//params[f.Signature.GetInputs()[0].Name] = 0
	//request := fc.NewCompositionRequest(shortuuid.New(), comp, params)
	//resultMap, err2 := comp.Invoke(request)
	//utils.AssertNil(t, err2)
	//
	//// checks the result
	//output := resultMap.Result[f.Signature.GetOutputs()[0].Name]
	//utils.AssertEquals(t, expectedResult, output.(int))
	//fmt.Println("Result: ", output)
	// TODO run the dag end expect the correct result
}

// / TestParsingChoiceFunctionDagWithDefaultFail verifies that a json file with three different choices is correctly parsed in a Dag with a Choice node and three simple nodes.
func TestParsingChoiceFunctionDagWithDefaultFail(t *testing.T) {
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
	comp, err := fc.FromASL("choice", false, body)
	utils.AssertNilMsg(t, err, "unable to parse json")

	// runs the workflow, making it going to the fail part
	params := make(map[string]interface{})
	params[incFn.Signature.GetInputs()[0].Name] = 0
	request := fc.NewCompositionRequest(shortuuid.New(), comp, params)
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
}

// 1st branch (input==1): inc + inc (expected nothing)
// 2nd branch (input==2): double + inc (expected nothing)
// def branch (true    ): hello (expected nothing)
func TestParsingChoiceDagWithDataTestExpr(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test")
	}
	// Creates "inc", "double" and "hello" python functions
	funcs := initializeAllPyFunctionFromNames(t, "inc", "double", "hello")

	// reads the file
	body, err := os.ReadFile("asl/choice_datatestexpr.json")
	utils.AssertNilMsg(t, err, "unable to read file")
	// parse the ASL language
	comp, err := fc.FromASL("choice3", false, body)
	utils.AssertNilMsg(t, err, "unable to parse json")

	incFn := funcs[0]
	// helloFn := funcs[len(funcs)-1]

	// runs the workflow (1st choice branch) test: (input == 1)
	fmt.Println("1st branch invocation (if input == 1)... ")
	params1 := make(map[string]interface{})
	params1[incFn.Signature.GetInputs()[0].Name] = 1
	request1 := fc.NewCompositionRequest(shortuuid.New(), comp, params1)
	resultMap1, err1 := comp.Invoke(request1)
	utils.AssertNil(t, err1)

	// checks that output is 1+1+1=3
	output := resultMap1.Result[incFn.Signature.GetOutputs()[0].Name]
	utils.AssertEquals(t, 3, output.(int))
	fmt.Println(resultMap1.String())
	fmt.Println("=============================================")
	// runs the workflow (2nd choice branch) test: (input == 2)
	fmt.Println("2nd branch invocation (else if input == 2)...")
	params2 := make(map[string]interface{})
	params2[incFn.Signature.GetInputs()[0].Name] = 2
	request2 := fc.NewCompositionRequest(shortuuid.New(), comp, params2)
	resultMap, err2 := comp.Invoke(request2)
	utils.AssertNil(t, err2)

	// check that output is 2*2+1 = 5
	output2 := resultMap.Result[incFn.Signature.GetOutputs()[0].Name]
	utils.AssertEquals(t, 5, output2.(int))
	fmt.Println("Result: ", output2)

	// runs the workflow (default choice branch)
	fmt.Println("=============================================")
	fmt.Println("Default branch invocation...")
	paramsDefault := make(map[string]interface{})
	paramsDefault[incFn.Signature.GetInputs()[0].Name] = "Giacomo"
	requestDefault := fc.NewCompositionRequest(shortuuid.New(), comp, paramsDefault)
	resultMap, errDef := comp.Invoke(requestDefault)
	utils.AssertNil(t, errDef)
	fmt.Printf("Composition Execution Report: %s\n", resultMap.String())
}

func TestParsingChoiceDagWithBoolExpr(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test")
	}

	// Creates "inc", "double" and "hello" python functions
	_ = initializeAllPyFunctionFromNames(t, "inc", "double", "hello")

	// reads the file
	body, err := os.ReadFile("asl/choice_boolexpr.json")
	utils.AssertNilMsg(t, err, "unable to read file")
	// parse the ASL language
	comp, err := fc.FromASL("choice2", false, body)
	utils.AssertNilMsg(t, err, "unable to parse json")

	// 1st branch (type != "Private")
	fmt.Println("1st branch: (type != 'Private') -> inc + inc")
	params := make(map[string]interface{})
	params["type"] = "Public"
	params["value"] = 1
	//params["input"] = 1
	request := fc.NewCompositionRequest(shortuuid.New(), comp, params)
	resultMap, err1 := comp.Invoke(request)
	utils.AssertNil(t, err1)

	// checks the result (1+1+1 = 3)
	output, err := resultMap.GetIntSingleResult()
	utils.AssertNilMsg(t, err, "failed to get int single result")
	utils.AssertEquals(t, 3, output)
	fmt.Println("Result: ", output)

	// 2nd branch (type == "Private", value is present, value is numeric, value >= 20, value < 30)
	fmt.Println("2nd branch: (type == \"Private\", value is present, value is numeric, value >= 20, value < 30) -> double + inc")
	params2 := make(map[string]interface{})
	params2["type"] = "Private"
	params2["value"] = 20
	request2 := fc.NewCompositionRequest(shortuuid.New(), comp, params2)
	resultMap2, err2 := comp.Invoke(request2)
	utils.AssertNil(t, err2)

	// checks the result (20*2+1 = 41)
	output2, err := resultMap2.GetIntSingleResult()
	utils.AssertNilMsg(t, err, "failed to get int single result")
	utils.AssertEquals(t, 41, output2)
	fmt.Println("Result: ", output2)

	// 2nd branch (type == "Private", value is present, value is numeric, value >= 20, value < 30)
	fmt.Println("default branch (we specify nothing instead of a number)")
	params3 := make(map[string]interface{})
	params3["type"] = "Private"
	request3 := fc.NewCompositionRequest(shortuuid.New(), comp, params3)
	resultMap3, err2 := comp.Invoke(request3)
	utils.AssertNil(t, err2)
	fmt.Printf("Composition Execution Report: %s\n", resultMap3.String())
	// no results to check
}

func TestParsingDagWithMalformedJson(t *testing.T) {}

func TestParsingDagWithUnknownFunction(t *testing.T) {}
