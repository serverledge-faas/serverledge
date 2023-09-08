package test

import (
	"encoding/json"
	"fmt"
	"github.com/grussorusso/serverledge/internal/function"
	u "github.com/grussorusso/serverledge/utils"
	"testing"
)

func TestMarshalSignature(t *testing.T) {
	sig := function.NewSignature().
		AddInput("hello", function.Text{}).
		AddInput("age", function.Int{}).
		AddOutput("price", function.Float{}).
		AddOutput("items", function.Array[function.Text]{DataType: function.Text{}}).
		Build()
	marshal, err := json.Marshal(*sig)
	u.AssertNil(t, err)
	u.AssertEquals(t, "{\"Inputs\":[{\"Name\":\"hello\",\"Type\":\"Text\"},{\"Name\":\"age\",\"Type\":\"Int\"}],\"Outputs\":[{\"Name\":\"price\",\"Type\":\"Float\"},{\"Name\":\"items\",\"Type\":\"ArrayText\"}]}", fmt.Sprintf("%s", marshal))
	fmt.Printf("Signature:\n%s\n", marshal)

	m, e := json.Marshal(function.InputDef{Name: "a", Type: "Int"})
	u.AssertNil(t, e)
	u.AssertEquals(t, "{\"Name\":\"a\",\"Type\":\"Int\"}", fmt.Sprintf("%s", m))
	fmt.Printf("InputDef:\n%s\n", m)
}

// InputDef test
func TestInputDef(t *testing.T) {
	i1 := function.InputDef{
		Name: "param1",
		Type: function.FLOAT,
	}
	inputMap := make(map[string]interface{})
	inputMap["fake"] = "fake"
	inputMap["param1"] = 1.5
	err := i1.CheckInput(inputMap)
	u.AssertNil(t, err)
}

// OutputDef test
func TestOutputDef(t *testing.T) {
	o1 := function.OutputDef{
		Name: "output1",
		Type: function.TEXT,
	}
	inputMap := make(map[string]interface{})
	inputMap["fake"] = 1

	err := o1.CheckOutput(inputMap)
	u.AssertNonNil(t, err)
	inputMap["output1"] = "ahaha"
	err = o1.CheckOutput(inputMap)
	u.AssertNil(t, err)
}

func TestSignatureInputOnly(t *testing.T) {
	// only input
	sig := function.NewSignature().
		AddInput("hello", function.Text{}).
		Build()
	m := make(map[string]interface{})
	// we do not have outputs
	u.AssertNil(t, sig.CheckAllOutputs(m))
	// check correct input
	m["hello"] = "giacomo"
	u.AssertNil(t, sig.CheckAllInputs(m))
	// check wrong input
	m["hello"] = []string{"no", "yes"}
	u.AssertNonNil(t, sig.CheckAllInputs(m))
	// check nil input value
	m["hello"] = nil
	u.AssertNonNil(t, sig.CheckAllInputs(m))
	// u.AssertNil(t, sig.CheckAllOutputs(make(map[string]interface{})))
}

func TestSignatureOutputOnly(t *testing.T) {
	// only input
	sig := function.NewSignature().
		AddOutput("len", function.Int{}).
		Build()
	m := make(map[string]interface{})
	// we do not have input
	u.AssertNil(t, sig.CheckAllInputs(m))
	// check correct type output
	m["len"] = 1
	u.AssertNil(t, sig.CheckAllOutputs(m))
	// check the type conversion
	m["len"] = "1"
	u.AssertNil(t, sig.CheckAllOutputs(m))
	// this should not be converted
	m["len"] = "fake"
	u.AssertNonNil(t, sig.CheckAllOutputs(m))
	// check nil input value
	m["len"] = nil
	u.AssertNonNil(t, sig.CheckAllOutputs(m))
}

func TestComplexSignature(t *testing.T) {
	// only input
	sig := function.NewSignature().
		AddInput("hello", function.Text{}).
		AddInput("age", function.Int{}).
		AddOutput("price", function.Float{}).
		AddOutput("items", function.Array[function.Text]{DataType: function.Text{}}).
		Build()
	m := make(map[string]interface{})
	// we do not have all inputs
	u.AssertNonNil(t, sig.CheckAllInputs(m))
	// we do not have all outputs
	u.AssertNonNil(t, sig.CheckAllOutputs(m))
	// check partially correct input
	m["hello"] = "giacomo"
	u.AssertNonNil(t, sig.CheckAllInputs(m))
	// check useless input
	m["food"] = "pizza"
	u.AssertNonNil(t, sig.CheckAllInputs(m))
	// check correct input, but also with useless input (should work)
	m["age"] = "26"
	u.AssertNil(t, sig.CheckAllInputs(m))
	// check totally correct input
	delete(m, "food")
	u.AssertNil(t, sig.CheckAllInputs(m))
	// check partial output (with conversion)
	m["price"] = "2.5"
	u.AssertNonNil(t, sig.CheckAllOutputs(m))
	// check wrong output
	m["items"] = []int{1, 2, 3, 4, 5}
	u.AssertNonNil(t, sig.CheckAllOutputs(m))
	// check correct output but with useless outputs (should work)
	m["items"] = []string{"1", "2", "3", "4", "5"}
	u.AssertNil(t, sig.CheckAllOutputs(m))
	// check fully correct output
	delete(m, "hello")
	delete(m, "age")
	u.AssertNil(t, sig.CheckAllOutputs(m))
}
