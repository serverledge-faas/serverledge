package asl

import (
	"fmt"

	"github.com/buger/jsonparser"
	"github.com/serverledge-faas/serverledge/internal/types"
)

type Formula interface {
	types.Comparable
	fmt.Stringer
	GetFormulaType() string
}

type AndFormula struct {
	And []*TestExpression
}

func (a *AndFormula) Equals(cmp types.Comparable) bool {
	otherAnd, ok := cmp.(*AndFormula)
	if !ok {
		return false
	}
	for i, choiceRule := range a.And {
		if !choiceRule.Equals(otherAnd.And[i]) {
			fmt.Printf("And1: %v\n And2: %v\n", choiceRule, otherAnd.And[i])
			return false
		}
	}

	return true
}
func printTestExpression(expressions []*TestExpression) string {
	str := "\t\t\t\t\t\n\t\t\t\t\t["
	exprLenMinusOne := len(expressions) - 1
	for i, choiceRule := range expressions {
		if i == 0 {
			str += "\n"
		}
		str += choiceRule.String()
		if i != exprLenMinusOne {
			str += ",\n"
		} else {
			str += "\n"
		}
	}

	str += "\t\t\t\t\t]"
	return str
}

func (a *AndFormula) String() string {
	return printTestExpression(a.And)
}

func (a *AndFormula) GetFormulaType() string {
	return "And"
}

func ParseAnd(jsonBytes []byte) (*AndFormula, error) {
	andJson, err := JsonExtract(jsonBytes, "And")
	if err != nil {
		return nil, fmt.Errorf("failed to parse and formula: %v", err)
	}
	andArray := make([]*TestExpression, 0)
	_, err = jsonparser.ArrayEach(andJson, func(value []byte, dataType jsonparser.ValueType, offset int, err error) {
		rule, err := ParseTestExpr(value)
		if err != nil {
			return
		}
		andArray = append(andArray, rule)
	})
	if err != nil {
		return nil, fmt.Errorf("failed to parse and formula: %v", err)
	}

	return &AndFormula{And: andArray}, nil
}

type OrFormula struct {
	Or []*TestExpression
}

func ParseOr(jsonBytes []byte) (*OrFormula, error) {
	orRule, err := JsonExtract(jsonBytes, "Or")
	if err != nil {
		return nil, fmt.Errorf("failed to parse Or formula: %v", err)
	}
	orArray := make([]*TestExpression, 0)
	_, err = jsonparser.ArrayEach(orRule, func(value []byte, dataType jsonparser.ValueType, offset int, err error) {
		rule, err := ParseTestExpr(orRule)
		if err != nil {
			return
		}
		orArray = append(orArray, rule)
	})
	if err != nil {
		return nil, fmt.Errorf("failed to parse Or formula: %v", err)
	}

	return &OrFormula{Or: orArray}, nil
}

func (o *OrFormula) Equals(cmp types.Comparable) bool {
	otherOr, ok := cmp.(*OrFormula)
	if !ok {
		return false
	}
	for i, choiceRule := range o.Or {
		if !choiceRule.Equals(otherOr.Or[i]) {
			return false
		}
	}

	return true
}

func (o *OrFormula) String() string {
	return printTestExpression(o.Or)
}

func (o *OrFormula) GetFormulaType() string {
	return "Or"
}

type NotFormula struct {
	Not *TestExpression
}

func ParseNot(jsonBytes []byte) (*NotFormula, error) {
	notJson, err := JsonExtract(jsonBytes, "Not")
	if err != nil {
		return nil, fmt.Errorf("failed to parse Not formula: %v", err)
	}
	notRule, err := ParseTestExpr(notJson)
	if err != nil {
		return nil, fmt.Errorf("failed to parse Not formula: %v", err)
	}
	return &NotFormula{Not: notRule}, nil
}

func (n *NotFormula) Equals(cmp types.Comparable) bool {
	otherNot, ok := cmp.(*NotFormula)
	if !ok {
		return false
	}
	return otherNot.Not.Equals(n.Not)
}

func (n *NotFormula) String() string {
	return fmt.Sprintf(
		"\n\t\t\t\t\tVariable: %s\n"+
			"\t\t\t\t\t%s: %s",
		n.Not.Variable, n.Not.ComparisonOperator.Kind, n.Not.ComparisonOperator.Operand)
}

func (n *NotFormula) GetFormulaType() string {
	return "Not"
}
