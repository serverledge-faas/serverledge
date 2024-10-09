package fc_test

import (
	"fmt"
	"os"
	"testing"

	"github.com/grussorusso/serverledge/internal/fc"
	"github.com/grussorusso/serverledge/internal/function"
	"github.com/grussorusso/serverledge/internal/test"
)

func InitializeIncFunction(t *testing.T) {
	f, err := test.InitializePyFunction("inc", "handler", function.NewSignature().
		AddInput("input", function.Int{}).
		AddOutput("result", function.Int{}).
		Build())
	if err != nil {
		t.FailNow()
	}
	f.SaveToEtcd()
}

func TestParsingSimple(t *testing.T) {
	/*
		if val, found := os.LookupEnv("INTEGRATION"); !found || val != "1" {
			t.SkipNow()
		}*/

	InitializeIncFunction(t)

	body, err := os.ReadFile("../../test/simple.json")
	if err != nil {
		t.Fatalf("unable to read file: %v", err)
	}
	comp, err := fc.FromASL("prova", false, body)
	if err != nil {
		fmt.Printf("%v\n", err)
		t.Fail()
	}
	fmt.Println(comp)

	comp.SaveToEtcd()

	all, err := fc.GetAllFC()
	fmt.Println(all)
}
func TestParsing(t *testing.T) {
	//if val, found := os.LookupEnv("INTEGRATION"); !found || val != "1" {
	//	t.SkipNow()
	//}

	//InitializeIncFunction(t)

	//body, err := os.ReadFile("../../test/simple.json")
	body, err := os.ReadFile("../../test/simple.json")
	if err != nil {
		t.Fatalf("unable to read file: %v", err)
	}

	sm, _ := fc.FromASL("prova", false, body)
	fmt.Printf("Found state machine:  %v\n", sm)

	fmt.Println()
}
