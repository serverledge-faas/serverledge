package test

import (
	"encoding/json"
	"testing"

	"github.com/serverledge-faas/serverledge/internal/workflow"
	"github.com/serverledge-faas/serverledge/utils"
)

var predicate1 = workflow.Predicate{Root: workflow.Condition{Type: workflow.And, Find: []bool{false, false}, Sub: []workflow.Condition{{Type: workflow.Eq, Op: []interface{}{2, 2}, Find: []bool{false, false}}, {Type: workflow.Greater, Op: []interface{}{4, 2}, Find: []bool{false, false}}}}}
var predicate2 = workflow.Predicate{Root: workflow.Condition{Type: workflow.Or, Find: []bool{false, false}, Sub: []workflow.Condition{{Type: workflow.Const, Op: []interface{}{true}, Find: []bool{false}}, {Type: workflow.Smaller, Op: []interface{}{4, 2}, Find: []bool{false, false}}}}}
var predicate3 = workflow.Predicate{Root: workflow.Condition{Type: workflow.Or, Find: []bool{false, false}, Sub: []workflow.Condition{predicate1.Root, {Type: workflow.Smaller, Op: []interface{}{4, 2}, Find: []bool{false, false}}}}}
var predicate4 = workflow.Predicate{Root: workflow.Condition{Type: workflow.Not, Find: []bool{false}, Sub: []workflow.Condition{{Type: workflow.IsEmpty, Op: []interface{}{1, 2, 3, 4}, Find: []bool{false}}}}}

func TestPredicateMarshal(t *testing.T) {

	predicates := []workflow.Predicate{predicate1, predicate2, predicate3, predicate4}
	for _, predicate := range predicates {
		val, err := json.Marshal(predicate)
		utils.AssertNil(t, err)

		var predicateTest workflow.Predicate
		errUnmarshal := json.Unmarshal(val, &predicateTest)
		utils.AssertNil(t, errUnmarshal)
		utils.AssertTrue(t, predicate.Equals(predicateTest))
	}
}

func TestPredicate(t *testing.T) {
	ok := predicate1.Test(nil)
	utils.AssertTrue(t, ok)

	ok2 := predicate2.Test(nil)
	utils.AssertTrue(t, ok2)

	ok3 := predicate3.Test(nil)
	utils.AssertTrue(t, ok3)

	ok4 := predicate4.Test(nil)
	utils.AssertTrue(t, ok4)
}

func TestPrintPredicate(t *testing.T) {
	str := predicate1.LogicString()
	utils.AssertEquals(t, "(2 == 2 && 4 > 2)", str)

	str2 := predicate2.LogicString()
	utils.AssertEquals(t, "(true || 4 < 2)", str2)

	str3 := predicate3.LogicString()
	utils.AssertEquals(t, "((2 == 2 && 4 > 2) || 4 < 2)", str3)

	str4 := predicate4.LogicString()
	utils.AssertEquals(t, "!(IsEmpty(1))", str4)
}

func TestBuilder(t *testing.T) {
	built1 := workflow.NewPredicate().And(
		workflow.NewEqCondition(2, 2),
		workflow.NewGreaterCondition(4, 2),
	).Build()

	utils.AssertTrue(t, built1.Equals(predicate1.Root))

	built2 := workflow.NewPredicate().Or(
		workflow.NewConstCondition(true),
		workflow.NewSmallerCondition(4, 2),
	).Build()

	utils.AssertTrue(t, built2.Equals(predicate2.Root))

	built3 := workflow.NewPredicate().Or(
		workflow.NewAnd(
			workflow.NewEqCondition(2, 2),
			workflow.NewGreaterCondition(4, 2),
		),
		workflow.NewSmallerCondition(4, 2),
	).Build()
	utils.AssertTrue(t, built3.Equals(predicate3.Root))

	built4 := workflow.NewPredicate().Not(
		workflow.NewEmptyCondition([]interface{}{1, 2, 3, 4}),
	).Build()

	utils.AssertTrue(t, built4.Equals(predicate4.Root))

}
