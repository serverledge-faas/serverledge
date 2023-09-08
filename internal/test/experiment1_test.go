package test

import (
	"fmt"
	"github.com/grussorusso/serverledge/internal/fc"
	"github.com/grussorusso/serverledge/internal/function"
	"github.com/grussorusso/serverledge/utils"
	"testing"
)

var Experiment bool //initialized in TestMain
// create a sequence of varying length and run the experiment for 10 minutes

func TestExperiment1(t *testing.T) {
	if !Experiment {
		t.Skip()
	}
	lengths := []int{1, 2, 4, 8, 16, 32}
	for _, length := range lengths {
		_, err := CreateNoopCompositionSequence(t, fmt.Sprintf("sequence_%d", length), "localhost", 1323, length)
		utils.AssertNilMsg(t, err, "failed to create composition")
	}

	// err2 := comp.Delete()
	// utils.AssertNilMsg(t, err2, "failed to delete composition and functions")

}

// TestCreateComposition tests the compose REST API that creates a new function composition
func CreateNoopCompositionSequence(t *testing.T, fcName string, host string, port int, length int) (*fc.FunctionComposition, error) {
	fn, err := InitializePyFunction("noop", "handler", function.NewSignature().Build())
	utils.AssertNilMsg(t, err, "failed to initialize function noop")

	fArr := make([]*function.Function, 0, length)
	for i := 0; i < length; i++ {
		fArr = append(fArr, fn)
	}

	dag, err := fc.CreateSequenceDag(fArr...)
	composition := fc.NewFC(fcName, *dag, []*function.Function{fn}, true)
	createCompositionApiTest(t, &composition, host, port)

	return &composition, nil
}
