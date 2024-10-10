package test

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"testing"

	"github.com/cornelk/hashmap"
	"github.com/grussorusso/serverledge/internal/cli"
	"github.com/grussorusso/serverledge/internal/client"
	"github.com/grussorusso/serverledge/internal/fc"
	"github.com/grussorusso/serverledge/internal/function"
	"github.com/grussorusso/serverledge/utils"
)

const PY_MEMORY = 20
const JS_MEMORY = 50

func initializeExamplePyFunction() (*function.Function, error) {
	srcPath := "../../examples/inc.py"
	srcContent, err := cli.ReadSourcesAsTar(srcPath)
	if err != nil {
		return nil, fmt.Errorf("cannot read python sources %s as tar: %v", srcPath, err)
	}
	encoded := base64.StdEncoding.EncodeToString(srcContent)
	f := function.Function{
		Name:            "inc",
		Runtime:         "python310",
		MemoryMB:        PY_MEMORY,
		CPUDemand:       1.0,
		Handler:         "inc.handler", // on python, for now is needed file name and handler name!!
		TarFunctionCode: encoded,
		Signature: function.NewSignature().
			AddInput("input", function.Int{}).
			AddOutput("result", function.Int{}).
			Build(),
	}

	return &f, nil
}

func initializeExampleJSFunction() (*function.Function, error) {
	srcPath := "../../examples/inc.js"
	srcContent, err := cli.ReadSourcesAsTar(srcPath)
	if err != nil {
		return nil, fmt.Errorf("cannot read js sources %s as tar: %v", srcPath, err)
	}
	encoded := base64.StdEncoding.EncodeToString(srcContent)
	f := function.Function{
		Name:            "inc",
		Runtime:         "nodejs17ng",
		MemoryMB:        JS_MEMORY,
		CPUDemand:       1.0,
		Handler:         "inc", // for js, only the file name is needed!!
		TarFunctionCode: encoded,
		Signature: function.NewSignature().
			AddInput("input", function.Int{}).
			AddOutput("result", function.Int{}).
			Build(),
	}

	return &f, nil
}

func InitializePyFunction(name string, handler string, sign *function.Signature) (*function.Function, error) {
	srcPath := "../../examples/" + name + ".py"
	srcContent, err := cli.ReadSourcesAsTar(srcPath)
	if err != nil {
		return nil, fmt.Errorf("cannot read python sources %s as tar: %v", srcPath, err)
	}
	encoded := base64.StdEncoding.EncodeToString(srcContent)
	f := function.Function{
		Name:            name,
		Runtime:         "python310",
		MemoryMB:        PY_MEMORY,
		CPUDemand:       0.25,
		Handler:         fmt.Sprintf("%s.%s", name, handler), // on python, for now is needed file name and handler name!!
		TarFunctionCode: encoded,
		Signature:       sign,
	}
	return &f, nil
}

func initializeJsFunction(name string, sign *function.Signature) (*function.Function, error) {
	srcPath := "../../examples/" + name + ".js"
	srcContent, err := cli.ReadSourcesAsTar(srcPath)
	if err != nil {
		return nil, fmt.Errorf("cannot read js sources %s as tar: %v", srcPath, err)
	}
	encoded := base64.StdEncoding.EncodeToString(srcContent)
	f := function.Function{
		Name:            name,
		Runtime:         "nodejs17ng",
		MemoryMB:        JS_MEMORY,
		CPUDemand:       0.25,
		Handler:         name, // on js only file name is needed!!
		TarFunctionCode: encoded,
		Signature:       sign,
	}
	return &f, nil
}

func initializePyFunctionFromName(t *testing.T, name string) *function.Function {
	var f *function.Function
	switch name {
	case "inc":
		f1, err := InitializePyFunction(name, "handler", function.NewSignature().
			AddInput("input", function.Int{}).
			AddOutput("result", function.Int{}).
			Build())
		utils.AssertNil(t, err)
		f = f1
	case "double":
		f2, err := InitializePyFunction(name, "handler", function.NewSignature().
			AddInput("input", function.Int{}).
			AddOutput("result", function.Int{}).
			Build())
		utils.AssertNil(t, err)
		f = f2
	case "hello":
		f3, err := InitializePyFunction(name, "handler", function.NewSignature().
			AddInput("input", function.Text{}).
			Build())
		utils.AssertNil(t, err)
		f = f3
	default: // inc
		fd, err := InitializePyFunction("inc", "handler", function.NewSignature().
			AddInput("input", function.Int{}).
			AddOutput("result", function.Int{}).
			Build())
		utils.AssertNil(t, err)
		f = fd
	}
	err := f.SaveToEtcd()
	utils.AssertNil(t, err)
	fmt.Printf("Created function %s\n", f.Name)
	return f
}

func initializeAllPyFunctionFromNames(t *testing.T, names ...string) []*function.Function {
	funcs := make([]*function.Function, 0)
	for _, name := range names {
		funcs = append(funcs, initializePyFunctionFromName(t, name))
	}
	return funcs
}

// parseFileName takes the name of the file, without .json and parses it. Produces the composition and a single function (for now)
func parseFileName(t *testing.T, rmFnOnDeletion bool, aslFileName string) (*fc.FunctionComposition, *function.Function) {
	f := initializePyFunctionFromName(t, "inc")

	body, err := os.ReadFile(fmt.Sprintf("asl/%s.json", aslFileName))
	utils.AssertNilMsg(t, err, "unable to read file")

	// for now, we use the same name as the filename to create the composition
	comp, err := fc.FromASL(aslFileName, rmFnOnDeletion, body)
	utils.AssertNilMsg(t, err, "unable to parse json")
	return comp, f
}

// initializeSameFunctionSlice is used to easily initialize a function array with one single function
func initializeSameFunctionSlice(length int, jsOrPy string) (*function.Function, []*function.Function, error) {
	var f *function.Function
	var err error
	if jsOrPy == "js" {
		f, err = initializeExampleJSFunction()
	} else if jsOrPy == "py" {
		f, err = initializeExamplePyFunction()
	} else {
		return nil, nil, fmt.Errorf("you can only choose from js or py (or custom runtime...)")
	}
	if err != nil {
		return f, nil, err
	}
	fArr := make([]*function.Function, length)
	for i := 0; i < length; i++ {
		fArr[i] = f
	}
	return f, fArr, nil
}

func createApiTest(t *testing.T, fn *function.Function, host string, port int) {
	marshaledFunc, err := json.Marshal(fn)
	utils.AssertNil(t, err)
	url := fmt.Sprintf("http://%s:%d/create", host, port)
	postJson, err := utils.PostJson(url, marshaledFunc)
	utils.AssertNil(t, err)

	utils.PrintJsonResponse(postJson.Body)
}

func invokeApiTest(fn string, params map[string]interface{}, host string, port int) error {
	request := client.InvocationRequest{
		Params: params,
		// QoSClass:        qosClass,
		QoSMaxRespT:     250,
		CanDoOffloading: true,
		Async:           false,
	}
	invocationBody, err1 := json.Marshal(request)
	if err1 != nil {
		return err1
	}
	url := fmt.Sprintf("http://%s:%d/invoke/%s", host, port, fn)
	_, err2 := utils.PostJson(url, invocationBody)
	if err2 != nil {
		return err2
	}
	// utils.PrintJsonResponse(resp.Body)
	return nil
}

func getFunctionApiTest(t *testing.T, host string, port int) []string {
	url := fmt.Sprintf("http://%s:%d/function", host, port)
	resp, err := http.Get(url)
	utils.AssertNil(t, err)
	var functionNames []string
	functionListJson := utils.GetJsonResponse(resp.Body)
	err = json.Unmarshal([]byte(functionListJson), &functionNames)
	utils.AssertNil(t, err)
	return functionNames
}

func deleteApiTest(t *testing.T, fn string, host string, port int) {
	request := function.Function{Name: fn}
	requestBody, err := json.Marshal(request)
	utils.AssertNil(t, err)

	url := fmt.Sprintf("http://%s:%d/delete", host, port)
	resp, err := utils.PostJson(url, requestBody)
	utils.AssertNil(t, err)

	utils.PrintJsonResponse(resp.Body)
}

func createCompositionApiTest(t *testing.T, fc *fc.FunctionComposition, host string, port int) {
	marshaledFunc, err := json.Marshal(fc)
	utils.AssertNilMsg(t, err, "failed to marshal composition")
	url := fmt.Sprintf("http://%s:%d/compose", host, port)
	postJson, err := utils.PostJson(url, marshaledFunc)
	utils.AssertNilMsg(t, err, "failed to create composition")

	utils.PrintJsonResponse(postJson.Body)
}

func invokeCompositionApiTest(t *testing.T, params map[string]interface{}, fc string, host string, port int, async bool) string {
	qosMap := make(map[string]function.RequestQoS)
	qosMap["grep"] = function.RequestQoS{
		Class:    0,
		MaxRespT: 500,
	}
	request := client.CompositionInvocationRequest{
		Params:          params,
		RequestQoSMap:   qosMap,
		CanDoOffloading: true,
		Async:           async,
	}
	invocationBody, err1 := json.Marshal(request)
	utils.AssertNilMsg(t, err1, "error while marshaling invocation request for composition")

	url := fmt.Sprintf("http://%s:%d/play/%s", host, port, fc)
	resp, err2 := utils.PostJson(url, invocationBody)
	utils.AssertNilMsg(t, err2, "error while posting json request for invoking a composition")
	return utils.GetJsonResponse(resp.Body)
}

func getCompositionsApiTest(t *testing.T, host string, port int) []string {
	url := fmt.Sprintf("http://%s:%d/fc", host, port)
	resp, err := http.Get(url)
	utils.AssertNil(t, err)
	var fcNames []string
	functionListJson := utils.GetJsonResponse(resp.Body)
	err = json.Unmarshal([]byte(functionListJson), &fcNames)
	utils.AssertNilMsg(t, err, "failed to get compositions")
	return fcNames
}

func deleteCompositionApiTest(t *testing.T, fcName string, host string, port int) {
	request := fc.FunctionComposition{Name: fcName}
	requestBody, err := json.Marshal(request)
	utils.AssertNilMsg(t, err, "failed to marshal composition to delete")

	url := fmt.Sprintf("http://%s:%d/uncompose", host, port)
	resp, err := utils.PostJson(url, requestBody)
	utils.AssertNilMsg(t, err, "failed to delete composition")

	utils.PrintJsonResponse(resp.Body)
}

func pollCompositionTest(t *testing.T, requestId string, host string, port int) string {
	url := fmt.Sprintf("http://%s:%d/poll/%s", host, port, requestId)
	resp, err := http.Get(url)
	utils.AssertNilMsg(t, err, "failed to poll invocation result")
	return utils.GetJsonResponse(resp.Body)
}

func newCompositionRequestTest() *fc.CompositionRequest {

	return &fc.CompositionRequest{
		ReqId: "test",
		ExecReport: fc.CompositionExecutionReport{
			Reports: hashmap.New[fc.ExecutionReportId, *function.ExecutionReport](), // make(map[fc.ExecutionReportId]*function.ExecutionReport),
		},
	}
}

func IsWindows() bool {
	return os.PathSeparator == '\\' && os.PathListSeparator == ';'
}
