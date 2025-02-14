package test

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"github.com/serverledge-faas/serverledge/internal/node"
	"net/http"
	"os"
	"testing"

	"github.com/serverledge-faas/serverledge/internal/cli"
	"github.com/serverledge-faas/serverledge/internal/client"
	"github.com/serverledge-faas/serverledge/internal/function"
	"github.com/serverledge-faas/serverledge/internal/workflow"
	"github.com/serverledge-faas/serverledge/utils"
)

const PY_MEMORY = 20
const JS_MEMORY = 50

func initializeExamplePyFunction() (*function.Function, error) {
	oldF, found := function.GetFunction("inc")
	if found {
		// the function already exists; we delete it
		oldF.Delete()
		node.ShutdownWarmContainersFor(oldF)
	}

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
	err = f.SaveToEtcd()

	return &f, err
}

func initializeExampleJSFunction() (*function.Function, error) {
	oldF, found := function.GetFunction("inc")
	if found {
		// the function already exists; we delete it
		oldF.Delete()
		node.ShutdownWarmContainersFor(oldF)
	}

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
	err = f.SaveToEtcd()
	return &f, err
}

func InitializePyFunction(name string, handler string, sign *function.Signature) (*function.Function, error) {

	oldF, found := function.GetFunction(name)
	if found {
		// the function already exists; we delete it
		oldF.Delete()
		node.ShutdownWarmContainersFor(oldF)
	}

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
	err = f.SaveToEtcd()
	return &f, err
}

func initializeJsFunction(name string, sign *function.Signature) (*function.Function, error) {
	oldF, found := function.GetFunction(name)
	if found {
		// the function already exists; we delete it
		oldF.Delete()
		node.ShutdownWarmContainersFor(oldF)
	}

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
	err = f.SaveToEtcd()
	return &f, err
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
			AddOutput("result", function.Text{}).
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

// parseFileName takes the name of the file, without .json and parses it
func parseFileName(t *testing.T, aslFileName string) *workflow.Workflow {
	body, err := os.ReadFile(fmt.Sprintf("asl/%s.json", aslFileName))
	utils.AssertNilMsg(t, err, "unable to read file")

	// for now, we use the same name as the filename to create the composition
	comp, err := workflow.FromASL(aslFileName, body)
	fmt.Println(err)
	utils.AssertNilMsg(t, err, "unable to parse json")
	return comp
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

func createApiIfNotExistsTest(t *testing.T, fn *function.Function, host string, port int) {
	marshaledFunc, err := json.Marshal(fn)
	utils.AssertNil(t, err)
	url := fmt.Sprintf("http://%s:%d/create", host, port)
	postJson, err := utils.PostJsonIgnore409(url, marshaledFunc)
	utils.AssertNil(t, err)

	utils.PrintJsonResponse(postJson.Body)
}

func invokeApiTest(fn string, params map[string]interface{}, host string, port int) error {
	request := client.InvocationRequest{
		Params:          params,
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
	return nil
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

func createWorkflowApiTest(fc *workflow.Workflow, host string, port int) error {
	marshaledFunc, err := json.Marshal(fc)
	if err != nil {
		return err
	}
	url := fmt.Sprintf("http://%s:%d/workflow/import", host, port)
	_, err = utils.PostJsonIgnore409(url, marshaledFunc)
	return err
}

func invokeWorkflowApiTest(t *testing.T, params map[string]interface{}, fc string, host string, port int, async bool) string {
	request := client.WorkflowInvocationRequest{
		Params: params,
		QoS: function.RequestQoS{
			Class:    0,
			MaxRespT: 500,
		},
		CanDoOffloading: true,
		Async:           async,
	}
	invocationBody, err1 := json.Marshal(request)
	utils.AssertNilMsg(t, err1, "error while marshaling invocation request for composition")

	url := fmt.Sprintf("http://%s:%d/workflow/invoke/%s", host, port, fc)
	resp, err2 := utils.PostJson(url, invocationBody)
	utils.AssertNilMsg(t, err2, "error while posting json request for invoking a composition")
	return utils.GetJsonResponse(resp.Body)
}

func deleteWorkflowApiTest(t *testing.T, fcName string, host string, port int) {
	request := workflow.Workflow{Name: fcName}
	requestBody, err := json.Marshal(request)
	utils.AssertNilMsg(t, err, "failed to marshal composition to delete")

	url := fmt.Sprintf("http://%s:%d/workflow/delete", host, port)
	resp, err := utils.PostJson(url, requestBody)
	utils.AssertNilMsg(t, err, "failed to delete composition")

	utils.PrintJsonResponse(resp.Body)
}

func pollWorkflowTest(t *testing.T, requestId string, host string, port int) string {
	url := fmt.Sprintf("http://%s:%d/poll/%s", host, port, requestId)
	resp, err := http.Get(url)
	utils.AssertNilMsg(t, err, "failed to poll invocation result")
	return utils.GetJsonResponse(resp.Body)
}

func IsWindows() bool {
	return os.PathSeparator == '\\' && os.PathListSeparator == ';'
}

// CreateSequenceWorkflow if successful, returns a workflow pointer with a sequence of Simple Tasks
func CreateSequenceWorkflow(funcs ...*function.Function) (*workflow.Workflow, error) {
	builder := workflow.NewBuilder()
	for _, f := range funcs {
		builder = builder.AddSimpleNode(f)
	}
	return builder.Build()
}

// CreateChoiceWorkflow if successful, returns a workflow with one Choice Node with each branch consisting of the same sub-workflow
func CreateChoiceWorkflow(dagger func() (*workflow.Workflow, error), condArr ...workflow.Condition) (*workflow.Workflow, error) {
	return workflow.NewBuilder().
		AddChoiceNode(condArr...).
		ForEachBranch(dagger).
		EndChoiceAndBuild()
}

// CreateScatterSingleFunctionWorkflow if successful, returns a workflow with one fan out, N simple node with the same function
// and then a fan in node that merges all the result in an array.
func CreateScatterSingleFunctionWorkflow(fun *function.Function, fanOutDegree int) (*workflow.Workflow, error) {
	return workflow.NewBuilder().
		AddScatterFanOutNode(fanOutDegree).
		ForEachParallelBranch(func() (*workflow.Workflow, error) { return CreateSequenceWorkflow(fun) }).
		AddFanInNode().
		Build()
}

// CreateBroadcastWorkflow if successful, returns a workflow with one fan out node, N simple nodes with different functions and a fan in node
// The number of branches is defined by the number of given functions
func CreateBroadcastWorkflow(dagger func() (*workflow.Workflow, error), fanOutDegree int) (*workflow.Workflow, error) {
	return workflow.NewBuilder().
		AddBroadcastFanOutNode(fanOutDegree).
		ForEachParallelBranch(dagger).
		AddFanInNode().
		Build()
}

// CreateBroadcastMultiFunctionWorkflow if successful, returns a workflow with one fan out node, each branch chained with a different workflow that run in parallel, and a fan in node.
// The number of branch is defined as the number of dagger functions.
func CreateBroadcastMultiFunctionWorkflow(dagger ...func() (*workflow.Workflow, error)) (*workflow.Workflow, error) {
	builder := workflow.NewBuilder().
		AddBroadcastFanOutNode(len(dagger))
	for _, dagFn := range dagger {
		builder = builder.NextFanOutBranch(dagFn())
	}
	return builder.
		AddFanInNode().
		Build()
}

func GetSingleResult(cer *workflow.ExecutionReport) (string, error) {
	if len(cer.Result) == 1 {
		for _, value := range cer.Result {
			return fmt.Sprintf("%v", value), nil
		}
	}
	return "", fmt.Errorf("there is not exactly one result: there are %d result(s)", len(cer.Result))
}

func GetIntSingleResult(cer *workflow.ExecutionReport) (int, error) {
	if len(cer.Result) == 1 {
		for _, value := range cer.Result {
			valueInt, ok := value.(int)
			if !ok {
				return 0, fmt.Errorf("value %v cannot be casted to int", value)
			}
			return valueInt, nil
		}
	}
	return 0, fmt.Errorf("there is not exactly one result: there are %d result(s)", len(cer.Result))
}
