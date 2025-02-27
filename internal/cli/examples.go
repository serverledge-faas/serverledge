package cli

import (
	"encoding/base64"
	"fmt"
	"github.com/grussorusso/serverledge/internal/function"
	"github.com/grussorusso/serverledge/internal/workflow"
)

// FIXME: unused function
// TODO: this file can be removed. I'll keep it if we are interested in using the following workflows in tests.
func exampleParsing(str string) (*workflow.Workflow, []*function.Function, error) {

	py, err := InitializePyFunction("inc", "handler", function.NewSignature().AddInput("input", function.Int{}).AddOutput("result", function.Int{}).Build())
	if err != nil {
		return nil, nil, err
	}

	switch str {
	case "sequence":
		workflow, errSequence := workflow.CreateSequenceWorkflow(py, py, py)
		return workflow, []*function.Function{py}, errSequence
	case "choice":
		workflow, errChoice := workflow.CreateChoiceWorkflow(workflow.LambdaSequenceWorkflow(py, py), workflow.NewConstCondition(false), workflow.NewConstCondition(true))
		return workflow, []*function.Function{py}, errChoice
	case "parallel":
		workflow, errParallel := workflow.CreateBroadcastWorkflow(workflow.LambdaSequenceWorkflow(py, py), 3)
		return workflow, []*function.Function{py}, errParallel
	case "multifn_sequence":
		funSlice := make([]*function.Function, 0)
		for i := 0; i < 10; i++ {
			f, err := InitializePyFunctionWithName(fmt.Sprintf("noop_%d", i), "noop", "handler", function.NewSignature().Build())
			if err != nil {
				return nil, nil, err
			}
			funSlice = append(funSlice, f)
		}
		workflow, errSequence := workflow.CreateSequenceWorkflow(funSlice...)
		return workflow, funSlice, errSequence
	case "complex":
		fnGrep, err1 := InitializePyFunction("grep", "handler", function.NewSignature().
			AddInput("InputText", function.Text{}).
			AddOutput("Rows", function.Array[function.Text]{}).
			Build())
		if err1 != nil {
			return nil, nil, err1
		}

		fnWordCount, err2 := InitializeJsFunction("wordCount", function.NewSignature().
			AddInput("InputText", function.Text{}).
			AddInput("Task", function.Bool{}). // should be true
			AddOutput("NumberOfWords", function.Int{}).
			Build())
		if err2 != nil {
			return nil, nil, err2
		}

		fnSummarize, err3 := InitializePyFunction("summarize", "handler", function.NewSignature().
			AddInput("InputText", function.Text{}).
			AddInput("Task", function.Bool{}). // should be false
			AddOutput("Summary", function.Text{}).
			Build())
		if err3 != nil {
			return nil, nil, err3
		}
		workflow, errComplex := workflow.NewBuilder().
			AddChoiceNode(
				workflow.NewEqParamCondition(workflow.NewParam("Task"), workflow.NewValue(true)),
				workflow.NewEqParamCondition(workflow.NewParam("Task"), workflow.NewValue(false)),
				workflow.NewConstCondition(true),
			).
			NextBranch(workflow.CreateSequenceWorkflow(fnWordCount)).
			NextBranch(workflow.CreateSequenceWorkflow(fnSummarize)).
			NextBranch(workflow.NewBuilder().
				AddScatterFanOutNode(2).
				ForEachParallelBranch(workflow.LambdaSequenceWorkflow(fnGrep)).
				AddFanInNode(workflow.AddToArrayEntry).
				Build()).
			EndChoiceAndBuild()
		return workflow, []*function.Function{fnGrep, fnWordCount, fnSummarize}, errComplex
	default:
		return nil, nil, fmt.Errorf("failed to parse workflow - use a default workflow like 'sequence', 'choice', 'parallel' or 'complex'")
	}
}

func InitializePyFunction(name string, handler string, sign *function.Signature) (*function.Function, error) {
	return InitializePyFunctionWithName(name, name, handler, sign)
}

func InitializePyFunctionWithName(fnName string, fileName string, handler string, sign *function.Signature) (*function.Function, error) {
	srcPath := "./examples/" + fileName + ".py"
	srcContent, err := ReadSourcesAsTar(srcPath)
	if err != nil {
		return nil, fmt.Errorf("cannot read python sources %s as tar: %v", srcPath, err)
	}
	encoded := base64.StdEncoding.EncodeToString(srcContent)
	PY_MEMORY := int64(20)
	f := function.Function{
		Name:            fnName,
		Runtime:         "python310",
		MemoryMB:        PY_MEMORY,
		CPUDemand:       0.25,
		Handler:         fmt.Sprintf("%s.%s", fileName, handler), // on python, for now is needed file name and handler name!!
		TarFunctionCode: encoded,
		Signature:       sign,
	}
	return &f, nil
}

func InitializeJsFunction(name string, sign *function.Signature) (*function.Function, error) {
	srcPath := "./examples/" + name + ".js"
	srcContent, err := ReadSourcesAsTar(srcPath)
	if err != nil {
		return nil, fmt.Errorf("cannot read js sources %s as tar: %v", srcPath, err)
	}
	encoded := base64.StdEncoding.EncodeToString(srcContent)
	JS_MEMORY := int64(50)
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
