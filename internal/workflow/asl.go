package workflow

import (
	"fmt"

	"github.com/serverledge-faas/serverledge/internal/asl"
	"github.com/serverledge-faas/serverledge/internal/function"
)

// FromASL parses a AWS State Language specification file and returns a Workflow with the corresponding Serverledge Workflow
// The name of the workflow should not be the file name by default, to avoid problems when adding the same workflow multiple times.
func FromASL(name string, aslSrc []byte) (*Workflow, error) {
	stateMachine, err := asl.ParseFrom(name, aslSrc)
	if err != nil {
		return nil, fmt.Errorf("could not parse the ASL file: %v", err)
	}

	nextStateName := stateMachine.StartAt
	nextState := stateMachine.States[nextStateName]
	workflow, err := buildingLoop(stateMachine, nextState, nextStateName)

	if err != nil {
		return nil, fmt.Errorf("failed to convert ASL State Machine to Serverledge Workflow: %v", err)
	}

	workflow.Name = name

	return workflow, nil
}

func buildingLoop(sm *asl.StateMachine, nextState asl.State, nextStateName string) (*Workflow, error) {
	builder := NewBuilder()
	isTerminal := false
	// forse questo va messo in un metodo a parte e riutilizzato per navigare i branch dei choice
	for !isTerminal {

		switch nextState.GetType() {
		case asl.Task:

			taskState := nextState.(*asl.TaskState)
			b, err := BuildFromTaskState(builder, taskState, nextStateName)
			if err != nil {
				return nil, fmt.Errorf("failed building SimpleTask from task state: %v", err)
			}
			builder = b
			nextState, nextStateName, isTerminal = findNextOrTerminate(taskState, sm)
			break
		case asl.Parallel:
			parallelState := nextState.(*asl.ParallelState)
			b, err := BuildFromParallelState(builder, parallelState, nextStateName)
			if err != nil {
				return nil, fmt.Errorf("failed building FanInTask and FanOutTask from ParallelState: %v", err)
			}
			builder = b
			nextState, nextStateName, isTerminal = findNextOrTerminate(parallelState, sm)
			break
		case asl.Map:
			mapState := nextState.(*asl.MapState)
			b, err := BuildFromMapState(builder, mapState, nextStateName)
			if err != nil {
				return nil, fmt.Errorf("failed building MapNode from Map state: %v", err) // TODO: MapNode doesn't exist
			}
			builder = b
			nextState, nextStateName, isTerminal = findNextOrTerminate(mapState, sm)
			break
		case asl.Pass:
			passState := nextState.(*asl.PassState)
			b, err := BuildFromPassState(builder, passState, nextStateName)
			if err != nil {
				return nil, fmt.Errorf("failed building SimplNode with function 'pass' from Pass state: %v", err)
			}
			builder = b
			nextState, nextStateName, isTerminal = findNextOrTerminate(passState, sm)
			break
		case asl.Wait:
			waitState := nextState.(*asl.WaitState)
			b, err := BuildFromWaitState(builder, waitState, nextStateName)
			if err != nil {
				return nil, fmt.Errorf("failed building SimpleTask with function 'wait' from Wait state: %v", err)
			}
			builder = b
			nextState, nextStateName, isTerminal = findNextOrTerminate(waitState, sm)
			break
		case asl.Choice:
			choiceState := nextState.(*asl.ChoiceState)
			// In this case, the choice state will automatically build the workflow, because it is terminal
			return BuildFromChoiceState(builder, choiceState, nextStateName, sm)
		case asl.Succeed:
			succeed := nextState.(*asl.SucceedState)
			return BuildFromSucceedState(builder, succeed, nextStateName)
		case asl.Fail:
			failState := nextState.(*asl.FailState)
			return BuildFromFailState(builder, failState, nextStateName)
		default:
			return nil, fmt.Errorf("unknown state type %s", nextState.GetType())
		}
	}
	return builder.Build()
}

// BuildFromTaskState adds a SimpleTask to the previous Node. The simple node will have id as specified by the name parameter
func BuildFromTaskState(builder *Builder, t *asl.TaskState, name string) (*Builder, error) {
	f, found := function.GetFunction(t.Resource) // Could have been used t.GetResources()[0], but it is better to avoid the array dereference
	if !found {
		return nil, fmt.Errorf("non existing function in workflow: %s", t.Resource)
	}
	builder = builder.AddSimpleNodeWithId(f, name)
	return builder, nil
}

// BuildFromChoiceState adds a ChoiceTask as defined in the ChoiceState, connects it to the previous Node, and TERMINATES the DAG
func BuildFromChoiceState(builder *Builder, c *asl.ChoiceState, name string, entireSM *asl.StateMachine) (*Workflow, error) {
	conds, err := BuildConditionFromRule(c.Choices)
	if err != nil {
		return nil, err
	}
	branchBuilder := builder.AddChoiceNode(conds...)

	// the choice state has two or more StateMachine(s) in it, one for each branch
	i := 0
	for branchBuilder.HasNextBranch() {
		var nextState string
		if i < len(conds)-1 {
			// choice branches
			nextState = c.Choices[i].GetNextState()
		} else {
			// we add one more branch to the ChoiceTask to handle the default branch

			nextState = c.Default
		}
		workflow, errBranch := GetBranchForChoiceFromStates(entireSM, nextState, i)
		if errBranch != nil {
			return nil, errBranch
		}
		branchBuilder = branchBuilder.NextBranch(workflow, errBranch)
		i++
	}
	return branchBuilder.EndChoiceAndBuild()
}

// BuildConditionFromRule creates a condition from a rule
func BuildConditionFromRule(rules []asl.ChoiceRule) ([]Condition, error) {
	conds := make([]Condition, 0)

	for i, rule := range rules {
		switch t := rule.(type) {
		case *asl.BooleanExpression:
			condition, err := buildBooleanExpr(t)
			if err != nil {
				return []Condition{}, fmt.Errorf("failed to build boolean expression %d: %v", i, err)
			}
			conds = append(conds, condition)
			break
		case *asl.DataTestExpression:
			condition, err := buildTestExpr(t.Test)
			if err != nil {
				return []Condition{}, fmt.Errorf("failed to build data test expression %d: %v", i, err)
			}
			conds = append(conds, condition)
			break
		default:
			return []Condition{}, fmt.Errorf("this is not a ChoiceRule: %v", rule)
		}
	}

	// this is for the default branch
	conds = append(conds, NewConstCondition(true))
	return conds, nil
}

func buildBooleanExpr(b *asl.BooleanExpression) (Condition, error) {
	var condition Condition
	switch t := b.Formula.(type) {
	case *asl.AndFormula:
		andConditions := make([]Condition, 0)
		for i, andExpr := range t.And {
			testExpr, err := buildTestExpr(andExpr)
			if err != nil {
				return NewConstCondition(false), fmt.Errorf("failed to build AND test expression %d %v:\n %v", i, t, err)
			}
			andConditions = append(andConditions, testExpr)
		}
		condition = NewAnd(andConditions...)
		break
	case *asl.OrFormula:
		orConditions := make([]Condition, 0)
		for i, orExpr := range t.Or {
			testExpr, err := buildTestExpr(orExpr)
			if err != nil {
				return NewConstCondition(false), fmt.Errorf("failed to build OR test expression %d %v:\n %v", i, t, err)
			}
			orConditions = append(orConditions, testExpr)
		}
		condition = NewAnd(orConditions...)
		break
	case *asl.NotFormula:
		testExpr, err := buildTestExpr(t.Not)
		if err != nil {
			return NewConstCondition(false), fmt.Errorf("failed to build NOT test expression %v:\n %v", t, err)
		}
		condition = NewNot(testExpr)
		break
	default:
		condition = NewConstCondition(false)
		break
	}
	return condition, nil
}

func buildTestExpr(t *asl.TestExpression) (Condition, error) {
	var param1, param2 *ParamOrValue
	// the Variable could be a parameter or a value, like "true", 1, etc.
	if asl.IsReferencePath(t.Variable) {
		param1 = NewParam(asl.RemoveDollar(t.Variable))
	} else {
		param1 = NewValue(t.Variable)
	}
	// The operand could be a constant or another parameter
	operand := t.ComparisonOperator.Operand
	if asl.IsReferencePath(operand) {
		operandPath, ok := operand.(string)
		if !ok {
			return NewConstCondition(false), fmt.Errorf("invalid comparison operator operand: it should have been a string")
		}
		param2 = NewParam(asl.RemoveDollar(operandPath))
	} else {
		param2 = NewValue(operand)
	}
	var condition Condition
	switch t.ComparisonOperator.Kind {
	case "StringEquals":
		condition = NewEqParamCondition(param1, param2)
	case "StringEqualsPath":
		condition = NewEqParamCondition(param1, param2)
	case "StringLessThan":
		condition = NewSmallerParamCondition(param1, param2)
	case "StringLessThanPath":
		condition = NewSmallerParamCondition(param1, param2)
	case "StringGreaterThan":
		condition = NewGreaterParamCondition(param1, param2)
	case "StringGreaterThanPath":
		condition = NewGreaterParamCondition(param1, param2)
	case "StringLessThanEquals":
		condition = NewOr(NewSmallerParamCondition(param1, param2), NewEqParamCondition(param1, param2))
	case "StringLessThanEqualsPath":
		condition = NewOr(NewSmallerParamCondition(param1, param2), NewEqParamCondition(param1, param2))
	case "StringGreaterThanEquals":
		condition = NewOr(NewGreaterParamCondition(param1, param2), NewEqParamCondition(param1, param2))
	case "StringGreaterThanEqualsPath":
		condition = NewOr(NewGreaterParamCondition(param1, param2), NewEqParamCondition(param1, param2))
	case "StringMatches":
		condition = NewStringMatchesParamCondition(param1, param2)
	case "NumericEquals":
		condition = NewEqParamCondition(param1, param2)
	case "NumericEqualsPath":
		condition = NewEqParamCondition(param1, param2)
	case "NumericLessThan":
		condition = NewSmallerParamCondition(param1, param2)
	case "NumericLessThanPath":
		condition = NewSmallerParamCondition(param1, param2)
	case "NumericGreaterThan":
		condition = NewGreaterParamCondition(param1, param2)
	case "NumericGreaterThanPath":
		condition = NewGreaterParamCondition(param1, param2)
	case "NumericLessThanEquals":
		condition = NewOr(NewSmallerParamCondition(param1, param2), NewEqParamCondition(param1, param2))
	case "NumericLessThanEqualsPath":
		condition = NewOr(NewSmallerParamCondition(param1, param2), NewEqParamCondition(param1, param2))
	case "NumericGreaterThanEquals":
		condition = NewOr(NewGreaterParamCondition(param1, param2), NewEqParamCondition(param1, param2))
	case "NumericGreaterThanEqualsPath":
		condition = NewOr(NewGreaterParamCondition(param1, param2), NewEqParamCondition(param1, param2))
	case "BooleanEquals":
		condition = NewEqParamCondition(param1, param2)
	case "BooleanEqualsPath":
		condition = NewEqParamCondition(param1, param2)
	case "TimestampEquals":
		condition = NewEqParamCondition(param1, param2)
	case "TimestampEqualsPath":
		condition = NewEqParamCondition(param1, param2)
	case "TimestampLessThan":
		condition = NewSmallerParamCondition(param1, param2)
	case "TimestampLessThanPath":
		condition = NewSmallerParamCondition(param1, param2)
	case "TimestampGreaterThan":
		condition = NewGreaterParamCondition(param1, param2)
	case "TimestampGreaterThanPath":
		condition = NewGreaterParamCondition(param1, param2)
	case "TimestampLessThanEquals":
		condition = NewOr(NewSmallerParamCondition(param1, param2), NewEqParamCondition(param1, param2))
	case "TimestampLessThanEqualsPath":
		condition = NewOr(NewSmallerParamCondition(param1, param2), NewEqParamCondition(param1, param2))
	case "TimestampGreaterThanEquals":
		condition = NewOr(NewGreaterParamCondition(param1, param2), NewEqParamCondition(param1, param2))
	case "TimestampGreaterThanEqualsPath":
		condition = NewOr(NewGreaterParamCondition(param1, param2), NewEqParamCondition(param1, param2))
	case "IsNull":
		condition = NewIsNullParamCondition(param1)
	case "IsPresent":
		condition = NewIsPresentParamCondition(param1)
	case "IsNumeric":
		condition = NewIsNumericParamCondition(param1)
	case "IsString":
		condition = NewIsStringParamCondition(param1)
	case "IsBoolean":
		condition = NewIsBooleanParamCondition(param1)
	case "IsTimestamp":
		condition = NewIsTimestampParamCondition(param1)
	}
	return condition, nil
}

func GetBranchForChoiceFromStates(sm *asl.StateMachine, nextState string, branchIndex int) (*Workflow, error) {
	return buildingLoop(sm, sm.States[nextState], nextState)
}

// BuildFromParallelState adds a FanOutTask and a FanInTask and as many branches as defined in the ParallelState
func BuildFromParallelState(builder *Builder, c *asl.ParallelState, name string) (*Builder, error) {
	// TODO: implement me
	return builder, nil
}

// BuildFromMapState is not compatible with Serverledge at the moment
func BuildFromMapState(builder *Builder, c *asl.MapState, name string) (*Builder, error) {
	// TODO: implement me
	// TODO: implement MapNode
	panic("not compatible with serverledge currently")
	// return builder, nil
}

// BuildFromPassState adds a SimpleTask with an identity function
func BuildFromPassState(builder *Builder, p *asl.PassState, name string) (*Builder, error) {
	// TODO: implement me
	return builder, nil
}

// BuildFromWaitState adds a Simple node with a sleep function for the specified time as described in the WaitState
func BuildFromWaitState(builder *Builder, w *asl.WaitState, name string) (*Builder, error) {
	// TODO: implement me
	return builder, nil
}

// BuildFromSucceedState adds a SuccessTask and an EndTask. When executing, the EndTask Result map will have the key 'Message' and if the message as value.
// If the message is "", it will have a generic success message.
func BuildFromSucceedState(builder *Builder, s *asl.SucceedState, name string) (*Workflow, error) {
	// 'Message' will be the key in the EndTask Result field
	// 'Execution completed successfully' will be the value in the EndTask Result field
	return builder.AddSucceedNodeAndBuild("Execution completed successfully")
}

// BuildFromFailState adds a FailureTask and an EndTask. When executing, the EndTask Result map will have the FailureTask Error as key and the FailureTask Cause as value.
// if error and cause are not specified, a GenericError key and a generic message will be set in the EndTask Result field.
func BuildFromFailState(builder *Builder, s *asl.FailState, name string) (*Workflow, error) {
	// Error or ErrorPath will be the key in the EndTask Result field
	// Cause oe CausePath will be the string value in the EndTask Result field.
	return builder.AddFailNodeAndBuild(s.GetError(), s.GetCause())
}
