package test

import (
	"encoding/base64"
	"fmt"
	"github.com/grussorusso/serverledge/internal/cli"
	"github.com/grussorusso/serverledge/internal/fc"
	"github.com/grussorusso/serverledge/internal/function"
	"github.com/grussorusso/serverledge/utils"
	"testing"
)

func TestExperiment3(t *testing.T) {
	if !Experiment {
		t.Skip()
	}
	_, err := createMultiFnComposition(t)
	utils.AssertNilMsg(t, err, "failed to create composition")

	// err2 := comp.Delete()
	// utils.AssertNilMsg(t, err2, "failed to delete composition and functions")
}

func createMultiFnComposition(t *testing.T) (*fc.FunctionComposition, error) {
	funSlice := make([]*function.Function, 0)
	for i := 0; i < 10; i++ {
		f, err := initializePyFunctionWithName(fmt.Sprintf("noop_%d", i), "noop", "handler", function.NewSignature().Build())
		utils.AssertNil(t, err)
		funSlice = append(funSlice, f)
	}
	dag, errSequence := fc.CreateSequenceDag(funSlice...)
	utils.AssertNil(t, errSequence)

	composition := fc.NewFC("complex", *dag, funSlice, true)
	createCompositionApiTest(t, &composition, "127.0.0.1", 1323)
	return &composition, nil
}

func initializePyFunctionWithName(fnName string, fileName string, handler string, sign *function.Signature) (*function.Function, error) {
	srcPath := "./examples/" + fileName + ".py"
	srcContent, err := cli.ReadSourcesAsTar(srcPath)
	if err != nil {
		return nil, fmt.Errorf("cannot read python sources %s as tar: %v", srcPath, err)
	}
	encoded := base64.StdEncoding.EncodeToString(srcContent)
	f := function.Function{
		Name:            fnName,
		Runtime:         "python310",
		MemoryMB:        PY_MEMORY,
		CPUDemand:       1.0,
		Handler:         fmt.Sprintf("%s.%s", fileName, handler), // on python, for now is needed file name and handler name!!
		TarFunctionCode: encoded,
		Signature:       sign,
	}
	return &f, nil
}
