package test

import (
	"testing"

	"github.com/serverledge-faas/serverledge/internal/asl"
	"github.com/serverledge-faas/serverledge/utils"
)

// TestParseTerminalChoiceWithThreeBranches tests that the following workflow...
//
//	   			[ Task ]
//	   			   |
//	   			[Choice]
//	   			   |
//	(input=1)--(input=2)--(default)
//	   |   		   |	     |
//
// [Task1] 		[Task2]   [Task3]
// [Task3] 		[Task3]
// ... is correctly parsed.
func TestParseChoiceWithDataTestExpr(t *testing.T) {
	choice := []byte(`{
		"Comment": "An example of the Amazon States Language using a choice state.",
		"StartAt": "ChoiceState",
		"States": {
			"ChoiceState": {
				"Type": "Choice",
				"Choices": [
					{
						"Variable": "input",
						"NumericEquals": 1,
						"Next": "FirstMatchState"
					},
					{
						"Variable": "input",
						"NumericEquals": 2,
						"Next": "SecondMatchState"
					}
				],
				"Default": "DefaultState"
			},
			"FirstMatchState": {
				"Comment": "Lang=Python",
				"Type": "Task",
				"Resource": "inc",
				"Next": "NextState"
			},	
			"SecondMatchState": {
				"Comment": "Lang=Python",
				"Type": "Task",
				"Resource": "double",
				"Next": "NextState"
			},
			
			"DefaultState": {
				"Comment": "Lang=Javascript",
				"Type": "Task",
				"Resource": "hello",
				"Next": "NextState"
			},
			
			"NextState": {
				"Comment": "Lang=Python",
				"Type": "Task",
				"Resource": "hello",
				"End": true
			}
		}
	}`)

	sm, err := asl.ParseFrom("choice", choice)
	utils.AssertNilMsg(t, err, "failed to parse state machine")

	smExpected := &asl.StateMachine{
		StartAt: "ChoiceState",
		Comment: "An example of the Amazon States Language using a choice state.",
		Version: "1.0",
		Name:    "choice",
		States: map[string]asl.State{
			"ChoiceState": &asl.ChoiceState{
				Type: asl.Choice,
				Choices: []asl.ChoiceRule{
					&asl.DataTestExpression{
						Test: &asl.TestExpression{
							Variable: "input",
							ComparisonOperator: &asl.ComparisonOperator{
								Kind:     asl.NumericEquals,
								DataType: asl.NumericComparator,
								Operand:  1,
							},
						},
						Next: "FirstMatchState",
					},
					&asl.DataTestExpression{
						Test: &asl.TestExpression{
							Variable: "input",
							ComparisonOperator: &asl.ComparisonOperator{
								Kind:     asl.NumericEquals,
								DataType: asl.NumericComparator,
								Operand:  2,
							},
						},
						Next: "SecondMatchState",
					},
				},
				InputPath:  "",
				OutputPath: "",
				Default:    "DefaultState",
			},
			"FirstMatchState":  asl.NewNonTerminalTask("inc", "NextState"),
			"SecondMatchState": asl.NewNonTerminalTask("double", "NextState"),
			"DefaultState":     asl.NewNonTerminalTask("hello", "NextState"),
			"NextState":        asl.NewTerminalTask("hello"),
		},
	}
	ok := smExpected.Equals(sm)
	utils.AssertTrueMsg(t, ok, "state machines differs")
}

// TestParseTerminalChoiceWithThreeBranches tests that the following workflow...
//
//	   [ Task ]
//	      |
//	   [Choice]
//	      |
//	(type!="Private")--(value is present && )----(default)
//				       (value is numeric && )	   |
//				       (value >= 20 &&      )	   |
//				       (value < 30          )	   |
//	     |              | 	                       |
//
// [Task Private]    [Task ValueInTwenties] 	 [Task DefaultState]
// ... is correctly parsed.
func TestParseChoiceWithBooleanExpr(t *testing.T) {

	choice := []byte(`{
		"Comment": "An example of the Amazon States Language using a choice state.",
		"StartAt": "ChoiceState",
		"States": {
			"ChoiceState": {
				"Type": "Choice",
				"Choices": [
					{
						"Not": {
							"Variable": "$.type",
							"StringEquals": "Private"
						},
						"Next": "Public"
    				},
    				{
      					"And": [
        					{
          						"Variable": "$.value",
          						"IsPresent": true
        					},
							{
							  "Variable": "$.value",
							  "IsNumeric": true
							},
							{
							  "Variable": "$.value",
							  "NumericGreaterThanEquals": 20
							},
							{
							  "Variable": "$.value",
							  "NumericLessThan": 30
							}
      					],
						"Next": "ValueInTwenties"
    				}
				],
				"Default": "DefaultState"
			},
			"Public": {
				"Comment": "Lang=Python",
				"Type": "Task",
				"Resource": "inc",
				"Next": "NextState"
			},	
			"ValueInTwenties": {
				"Comment": "Lang=Python",
				"Type": "Task",
				"Resource": "double",
				"Next": "NextState"
			},
			
			"DefaultState": {
				"Comment": "Lang=Javascript",
				"Type": "Task",
				"Resource": "hello",
				"Next": "NextState"
			},
			
			"NextState": {
				"Comment": "Lang=Python",
				"Type": "Task",
				"Resource": "hello",
				"End": true
			}
		}
	}`)

	sm, err := asl.ParseFrom("choice", choice)
	utils.AssertNilMsg(t, err, "failed to parse state machine")

	smExpected := &asl.StateMachine{
		StartAt: "ChoiceState",
		Comment: "An example of the Amazon States Language using a choice state.",
		Version: "1.0",
		Name:    "choice",
		States: map[string]asl.State{
			"ChoiceState": &asl.ChoiceState{
				Type: asl.Choice,
				Choices: []asl.ChoiceRule{
					&asl.BooleanExpression{
						Formula: &asl.NotFormula{
							Not: &asl.TestExpression{
								Variable: "$.type",
								ComparisonOperator: &asl.ComparisonOperator{
									Kind:     asl.StringEquals,
									DataType: asl.StringComparator,
									Operand:  "Private",
								},
							},
						},
						Next: "Public",
					},
					&asl.BooleanExpression{
						Formula: &asl.AndFormula{
							And: []*asl.TestExpression{
								{
									Variable: "$.value",
									ComparisonOperator: &asl.ComparisonOperator{
										Kind:     asl.IsPresent,
										DataType: asl.BooleanComparator,
										Operand:  true,
									},
								},
								{
									Variable: "$.value",
									ComparisonOperator: &asl.ComparisonOperator{
										Kind:     asl.IsNumeric,
										DataType: asl.BooleanComparator,
										Operand:  true,
									},
								},
								{
									Variable: "$.value",
									ComparisonOperator: &asl.ComparisonOperator{
										Kind:     asl.NumericGreaterThanEquals,
										DataType: asl.NumericComparator,
										Operand:  20,
									},
								},
								{
									Variable: "$.value",
									ComparisonOperator: &asl.ComparisonOperator{
										Kind:     asl.NumericLessThan,
										DataType: asl.NumericComparator,
										Operand:  30,
									},
								},
							},
						},
						Next: "ValueInTwenties",
					},
				},
				InputPath:  "",
				OutputPath: "",
				Default:    "DefaultState",
			},
			"Public":          asl.NewNonTerminalTask("inc", "NextState"),
			"ValueInTwenties": asl.NewNonTerminalTask("double", "NextState"),
			"DefaultState":    asl.NewNonTerminalTask("hello", "NextState"),
			"NextState":       asl.NewTerminalTask("hello"),
		},
	}
	ok := smExpected.Equals(sm)
	utils.AssertTrueMsg(t, ok, "state machines differs")
}
