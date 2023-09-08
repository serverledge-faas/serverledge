package cli

import (
	"encoding/base64"
	"fmt"
	"github.com/grussorusso/serverledge/internal/fc"
	"github.com/grussorusso/serverledge/internal/function"
)

func exampleParsing(str string) (*fc.Dag, []*function.Function, error) {

	py, err := InitializePyFunction("inc", "handler", function.NewSignature().AddInput("input", function.Int{}).AddOutput("result", function.Int{}).Build())
	if err != nil {
		return nil, nil, err
	}

	switch str {
	case "sequence":
		dag, errSequence := fc.CreateSequenceDag(py, py, py)
		return dag, []*function.Function{py}, errSequence
	case "choice":
		dag, errChoice := fc.CreateChoiceDag(fc.LambdaSequenceDag(py, py), fc.NewConstCondition(false), fc.NewConstCondition(true))
		return dag, []*function.Function{py}, errChoice
	case "parallel":
		dag, errParallel := fc.CreateBroadcastDag(fc.LambdaSequenceDag(py, py), 3)
		return dag, []*function.Function{py}, errParallel
	case "multifn_sequence":
		funSlice := make([]*function.Function, 0)
		for i := 0; i < 10; i++ {
			f, err := InitializePyFunctionWithName(fmt.Sprintf("noop_%d", i), "noop", "handler", function.NewSignature().Build())
			if err != nil {
				return nil, nil, err
			}
			funSlice = append(funSlice, f)
		}
		dag, errSequence := fc.CreateSequenceDag(funSlice...)
		return dag, funSlice, errSequence
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
		dag, errComplex := fc.NewDagBuilder().
			AddChoiceNode(
				fc.NewEqParamCondition(fc.NewParam("Task"), fc.NewValue(true)),
				fc.NewEqParamCondition(fc.NewParam("Task"), fc.NewValue(false)),
				fc.NewConstCondition(true),
			).
			NextBranch(fc.CreateSequenceDag(fnWordCount)).
			NextBranch(fc.CreateSequenceDag(fnSummarize)).
			NextBranch(fc.NewDagBuilder().
				AddScatterFanOutNode(2).
				ForEachParallelBranch(fc.LambdaSequenceDag(fnGrep)).
				AddFanInNode(fc.AddToArrayEntry).
				Build()).
			EndChoiceAndBuild()
		return dag, []*function.Function{fnGrep, fnWordCount, fnSummarize}, errComplex
	default:
		return nil, nil, fmt.Errorf("failed to parse dag - use a default dag like 'sequence', 'choice', 'parallel' or 'complex'")
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
