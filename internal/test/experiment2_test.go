package test

import (
	"github.com/grussorusso/serverledge/internal/fc"
	"github.com/grussorusso/serverledge/internal/function"
	"github.com/grussorusso/serverledge/utils"
	"testing"
)

func TestExperiment2(t *testing.T) {
	if !Experiment {
		t.Skip()
	}
	_, err := createComplexComposition(t)
	utils.AssertNilMsg(t, err, "failed to create composition")

	// err2 := comp.Delete()
	// utils.AssertNilMsg(t, err2, "failed to delete composition and functions")
}

/*
 * This test will execute the following Dag:
 *		        Start
 *		          |
 *		        Choice
 * Task==true  Task==false  true
 *    |            |          |
 *  WordCount   Summarize   Fan Out
 *    |            |        |     |
 *    |            |      Grep   Grep
 *    |            |        |     |
 *    |            |        Fan In
 *    |            |           |
 *    ------------End-----------
 */
func createComplexComposition(t *testing.T) (*fc.FunctionComposition, error) {
	fnGrep, err := InitializePyFunction("grep", "handler", function.NewSignature().
		AddInput("InputText", function.Text{}).
		AddOutput("Rows", function.Array[function.Text]{}).
		Build())
	utils.AssertNilMsg(t, err, "failed to initialize function noop")

	fnWordCount, err := initializeJsFunction("wordCount", function.NewSignature().
		AddInput("InputText", function.Text{}).
		AddInput("Task", function.Bool{}). // should be true
		AddOutput("NumberOfWords", function.Int{}).
		Build())

	fnSummarize, err := InitializePyFunction("summarize", "handler", function.NewSignature().
		AddInput("InputText", function.Text{}).
		AddInput("Task", function.Bool{}). // should be false
		AddOutput("Summary", function.Text{}).
		Build())

	dag, err := fc.NewDagBuilder().
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

	composition := fc.NewFC("complex", *dag, []*function.Function{fnWordCount, fnSummarize, fnGrep}, true)
	createCompositionApiTest(t, &composition, "127.0.0.1", 1323)
	return &composition, nil
}
