package test

import (
	"testing"

	"github.com/serverledge-faas/serverledge/internal/asl"
	"github.com/serverledge-faas/serverledge/utils"
)

func TestParseSingleTerminalTask(t *testing.T) {

	simple := []byte(`{
	  	"Comment": "A simple state machine with 1 task state",
	  	"StartAt": "FirstState",
	  	"States": {
	    	"FirstState": {
			  "Comment": "The first task",
			  "Type": "Task",
			  "Resource": "inc",
			  "End": true
	    	},
	  	}
	}`)

	sm, err := asl.ParseFrom("simple", simple)
	utils.AssertNilMsg(t, err, "failed to parse state machine")

	smExpected := &asl.StateMachine{
		StartAt: "FirstState",
		Comment: "A simple state machine with 1 task state",
		Version: "1.0",
		Name:    "simple",
		States: map[string]asl.State{
			"FirstState": asl.NewTerminalTask("inc"),
		},
	}
	utils.AssertTrueMsg(t, smExpected.Equals(sm), "state machines differs")
}

func TestParseTwoTask(t *testing.T) {

	simple := []byte(`{
	  	"Comment": "A simple state machine with 2 task state",
	  	"StartAt": "FirstState",
	  	"States": {
	    	"FirstState": {
			  	"Comment": "The first task",
			  	"Type": "Task",
			  	"Resource": "inc",
				"Next": "SecondState",
	    	},
			"SecondState": {
			  "Comment": "The second task",
			  "Type": "Task",
			  "Resource": "inc",
			  "End": true
	    	},
	  	}
	}`)

	sm, err := asl.ParseFrom("simple", simple)
	utils.AssertNilMsg(t, err, "failed to parse state machine")

	smExpected := &asl.StateMachine{
		StartAt: "FirstState",
		Comment: "A simple state machine with 2 task state",
		Version: "1.0",
		Name:    "simple",
		States: map[string]asl.State{
			"FirstState":  asl.NewNonTerminalTask("inc", "SecondState"),
			"SecondState": asl.NewTerminalTask("inc"),
		},
	}
	utils.AssertTrueMsg(t, smExpected.Equals(sm), "state machines differs")
}

func TestParseTwoTaskInverted(t *testing.T) {

	simple := []byte(`{
	  	"Comment": "A simple state machine with 2 task state",
	  	"StartAt": "FirstState",
	  	"States": {
			"SecondState": {
			  "Comment": "The second task",
			  "Type": "Task",
			  "Resource": "inc",
			  "End": true
	    	},
			"FirstState": {
			  	"Comment": "The first task",
			  	"Type": "Task",
			  	"Resource": "inc",
				"Next": "SecondState",
	    	},
	  	}
	}`)

	sm, err := asl.ParseFrom("simple", simple)
	utils.AssertNilMsg(t, err, "failed to parse state machine")

	smExpected := &asl.StateMachine{
		StartAt: "FirstState",
		Comment: "A simple state machine with 2 task state",
		Version: "1.0",
		Name:    "simple",
		States: map[string]asl.State{
			"FirstState":  asl.NewNonTerminalTask("inc", "SecondState"),
			"SecondState": asl.NewTerminalTask("inc"),
		},
	}
	utils.AssertTrueMsg(t, smExpected.Equals(sm), "state machines differs")
}

func TestParseTaskWithoutEnd(t *testing.T) {
	simple := []byte(`{
	  	"Comment": "A simple state machine with 2 task state",
	  	"StartAt": "FirstState",
	  	"States": {
			"FirstState": {
			  	"Comment": "The first task",
			  	"Type": "Task",
			  	"Resource": "inc",
	    	},
	  	}
	}`)

	sm, err1 := asl.ParseFrom("simple", simple)
	utils.AssertNilMsg(t, err1, "parsing failed")
	err := sm.Validate(sm.GetAllStateNames())
	utils.AssertNonNilMsg(t, err, "parsing should have failed")
}

func TestParseTaskWithInvalidHeartbeat(t *testing.T) {
	simple := []byte(`{
	  	"Comment": "A simple state machine with 2 task state",
	  	"StartAt": "FirstState",
	  	"States": {
			"FirstState": {
			  	"Comment": "The first task",
			  	"Type": "Task",
			  	"Resource": "inc",
				"TimeoutSeconds": 5,
				"HeartbeatSeconds": 10,
			    "End": true
	    	},
	  	}
	}`)

	sm, err1 := asl.ParseFrom("simple", simple)
	utils.AssertNilMsg(t, err1, "parsing failed")
	err := sm.Validate(sm.GetAllStateNames())
	utils.AssertNonNilMsg(t, err, "parsing should have failed")
}

func TestParseTaskWithBothPathAndHeartbeat(t *testing.T) {
	simple := []byte(`{
	  	"Comment": "A simple state machine with 2 task state",
	  	"StartAt": "FirstState",
	  	"States": {
			"FirstState": {
			  	"Comment": "The first task",
			  	"Type": "Task",
			  	"Resource": "inc",
				"HeartbeatSecondsPath": "$.inc",
				"HeartbeatSeconds": 10,
			    "End": true
	    	},
	  	}
	}`)

	sm, err1 := asl.ParseFrom("simple", simple)
	utils.AssertNilMsg(t, err1, "parsing failed")
	err := sm.Validate(sm.GetAllStateNames())
	utils.AssertNonNilMsg(t, err, "parsing should have failed")
}

func TestParseTaskWithOnlyHeartbeat(t *testing.T) {
	simple := []byte(`{
	  	"Comment": "A simple state machine with 2 task state",
	  	"StartAt": "FirstState",
	  	"States": {
			"FirstState": {
			  	"Comment": "The first task",
			  	"Type": "Task",
			  	"Resource": "inc",
				"HeartbeatSeconds": 10,
			    "End": true
	    	},
	  	}
	}`)

	sm, err1 := asl.ParseFrom("simple", simple)
	utils.AssertNilMsg(t, err1, "parsing failed")
	err := sm.Validate(sm.GetAllStateNames())
	utils.AssertNonNilMsg(t, err, "parsing should have failed")
}
