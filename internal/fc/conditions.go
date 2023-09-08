package fc

import (
	"fmt"
	"github.com/grussorusso/serverledge/internal/function"
	"github.com/grussorusso/serverledge/utils"
)

type Predicate struct {
	Root Condition
}

type Condition struct {
	Type CondEnum      `json:"Type"` // Type of the condition
	Find []bool        `json:"Find"` // Find if true, the corresponding Op value should be read from parameters
	Op   []interface{} `json:"Op"`   // Op is the list of operand of the condition
	Sub  []Condition   `json:"Sub"`  // Sub is a SubCondition List. Useful for Type And, Or and Not
}

type CondEnum int

const (
	And = iota
	Or
	Not
	Const
	Eq
	Diff
	Greater
	Smaller
	Empty // for collections
)

func (p Predicate) Test(input map[string]interface{}) bool {
	ok, err := p.Root.Test(input)
	if err != nil {
		fmt.Printf("%s\n", err.Error())
	}
	return ok
}

func (p Predicate) LogicString() string {
	return p.Root.ToString()
}

func (c Condition) ToString() string {
	switch c.Type {
	case And:
		str := "("
		for i, condition := range c.Sub {
			str += condition.ToString()
			if i != len(c.Sub)-1 {
				str += " && "
			}
		}
		str += ")"
		return str
	case Or:
		str := "("
		for i, condition := range c.Sub {
			str += fmt.Sprintf("%s", condition.ToString())
			if i != len(c.Sub)-1 {
				str += " || "
			}
		}
		str += ")"
		return str
	case Not:
		return fmt.Sprintf("!(%s)", c.Sub[0].ToString())
	case Const:
		if len(c.Op) == 0 {
			return "?"
		}
		return fmt.Sprintf("%v", c.Op[0])
	case Eq:
		if len(c.Op) == 1 {
			return fmt.Sprintf("%v == ?", c.Op[0])
		} else if len(c.Op) == 0 {
			return "? == ?"
		}
		return fmt.Sprintf("%v == %v", c.Op[0], c.Op[1])
	case Diff:
		if len(c.Op) == 1 {
			return fmt.Sprintf("%v != ?", c.Op[0])
		} else if len(c.Op) == 0 {
			return "? != ?"
		}
		return fmt.Sprintf("%v != %v", c.Op[0], c.Op[1])
	case Greater:
		if len(c.Op) == 1 {
			return fmt.Sprintf("%v > ?", c.Op[0])
		} else if len(c.Op) == 0 {
			return "? > ?"
		}
		return fmt.Sprintf("%v > %v", c.Op[0], c.Op[1])
	case Smaller:
		if len(c.Op) == 1 {
			return fmt.Sprintf("%v < ?", c.Op[0])
		} else if len(c.Op) == 0 {
			return "? < ?"
		}
		return fmt.Sprintf("%v < %v", c.Op[0], c.Op[1])
	case Empty:
		return "empty input"
	default:
		return ""
	}
}

func (p Predicate) Print() {
	fmt.Println(p.LogicString())
}

func (c Condition) findInputs(input map[string]interface{}) ([]interface{}, error) {
	ops := make([]interface{}, 0)
	if input == nil {
		return c.Op, nil
	}
	if len(c.Op) != len(c.Find) {
		return nil, fmt.Errorf("size of operand (%d) is different from size of searchable operands array (%d)", len(c.Op), len(c.Find))
	}
	for i := 0; i < len(c.Op); i++ {
		if !c.Find[i] {
			ops = append(ops, c.Op[i])
		} else {
			opStr, ok := c.Op[i].(string)
			if !ok {
				return nil, fmt.Errorf("input name is not a string")
			}
			value, found := input[opStr]
			if !found {
				ops = append(ops, false)
			}
			ops = append(ops, value)
		}
	}
	return ops, nil
}

func (c Condition) Test(input map[string]interface{}) (bool, error) {
	switch c.Type {
	case And:
		and := true
		for _, condition := range c.Sub {
			ok, err := condition.Test(input)
			if err != nil {
				return false, err
			}
			and = and && ok
		}
		return and, nil
	case Or:
		or := false
		for _, condition := range c.Sub {
			ok, err := condition.Test(input)
			if err != nil {
				return false, err
			}
			or = or || ok
		}
		return or, nil
	case Not:
		if len(c.Sub) != 1 {
			return false, fmt.Errorf("you need exactly one condition to check if it is Not satisfied")
		}

		test, err := c.Sub[0].Test(input)
		return !test, err
	case Const:
		if len(c.Op) == 0 {
			return false, fmt.Errorf("you need at least one const operand")
		}
		dataType := function.Bool{}
		ops, err := c.findInputs(input)
		if err != nil {
			return false, err
		}
		return dataType.Convert(ops[0])
	case Eq:
		if len(c.Op) <= 1 {
			return false, fmt.Errorf("you need at least two operands to check equality")
		}
		ops, err := c.findInputs(input)
		if err != nil {
			return false, err
		}
		for i := 0; i < len(ops)-1; i++ {
			if ops[i] != ops[i+1] {
				return false, nil
			}
		}
		return true, nil
	case Diff:
		if len(c.Op) <= 1 {
			return false, fmt.Errorf("you need at least two operands to check equality")
		}
		ops, err := c.findInputs(input)
		if err != nil {
			return false, err
		}
		for i := 0; i < len(ops)-1; i++ {
			if ops[i] == ops[i+1] {
				return false, nil
			}
		}
		return true, nil
	case Greater:
		if len(c.Op) != 2 {
			return false, fmt.Errorf("you need exactly two numbers to check which is greater")
		}
		f := function.Float{}
		ops, err := c.findInputs(input)
		if err != nil {
			return false, err
		}
		float1, err1 := f.Convert(ops[0])
		float2, err2 := f.Convert(ops[1])
		if err1 != nil || err2 != nil {
			return false, fmt.Errorf("not all operands %v can be converted to float64", c.Op)
		}
		return float1 > float2, nil
	case Smaller:
		if len(c.Op) != 2 {
			return false, fmt.Errorf("you need exactly two numbers to check which is greater")
		}
		f := function.Float{}
		ops, err := c.findInputs(input)
		if err != nil {
			return false, err
		}
		float1, err1 := f.Convert(ops[0])
		float2, err2 := f.Convert(ops[1])
		if err1 != nil || err2 != nil {
			return false, fmt.Errorf("not all operands %v can be converted to float64", c.Op)
		}
		return float1 < float2, nil
	case Empty:
		ops, err := c.findInputs(input)
		if err != nil {
			return false, err
		}
		slice, err := utils.ConvertToSlice(ops[0])
		if err != nil {
			return false, err
		}
		return len(slice) == 0, nil
	default:
		return false, fmt.Errorf("invalid operation condition")
	}
}

func (p Predicate) Equals(o Predicate) bool {
	return p.Root.Equals(o.Root)
}
func (c Condition) Equals(o Condition) bool {
	typeOk := c.Type == o.Type
	if !typeOk {
		fmt.Printf("operand type is not the same: %d vs %d\n", c.Type, o.Type)
	}
	lenOpOk := len(c.Op) == len(o.Op)
	if !lenOpOk {
		fmt.Printf("operand size is not the same: %d vs %d\n", len(c.Op), len(o.Op))
	}
	lenFindOk := len(c.Find) == len(o.Find)
	if len(c.Op) > 0 && len(o.Op) > 0 && lenOpOk {
		for i := range c.Op {
			f := function.Float{}

			val1, err1 := f.Convert(c.Op[i])
			val2, err2 := f.Convert(o.Op[i])

			// checking value of Op elements (converting to float because JSON treats all number as floats)
			if val1 != val2 {
				if err1 != nil {
					fmt.Printf("convertion error1: %v", err1)
				}
				if err2 != nil {
					fmt.Printf("convertion error2: %v", err2)
				}
				return false
			}
		}
	}

	lenSubCondOk := len(c.Sub) == len(o.Sub)
	if !lenSubCondOk {
		fmt.Printf("subconditions size is not the same: %d vs %d\n", len(c.Sub), len(o.Sub))
	}
	subOk := true
	if lenSubCondOk {
		for i := range c.Sub { // eq/const/ non ha sotto condizioni
			subOk = subOk && c.Sub[i].Equals(o.Sub[i])
		}
	}

	return typeOk && lenOpOk && lenSubCondOk && lenFindOk && subOk
}

func NewConstCondition(val interface{}) Condition {
	b := function.Bool{}

	err := b.TypeCheck(val)
	if err != nil {
		return NewConstCondition(false)
	}

	return Condition{
		Type: Const,
		Op:   []interface{}{val},
		Find: []bool{false},
	}
}

func NewAnd(conditions ...Condition) Condition {
	return Condition{
		Type: And,
		Sub:  conditions,
		Find: make([]bool, len(conditions)), // all false
	}
}

func NewOr(conditions ...Condition) Condition {
	return Condition{
		Type: Or,
		Sub:  conditions,
		Find: make([]bool, len(conditions)), // all false
	}
}

func NewNot(condition Condition) Condition {
	return Condition{
		Type: Not,
		Sub:  []Condition{condition},
		Find: make([]bool, 1), // all false
	}
}

// operations
func NewEqCondition(val1 interface{}, val2 interface{}) Condition {
	return Condition{
		Type: Eq,
		Op:   []interface{}{val1, val2},
		Find: make([]bool, 2), // all false
	}
}

type ParamOrValue struct {
	IsParam bool
	name    string
	value   interface{}
}

func NewParam(name string) *ParamOrValue {
	return &ParamOrValue{
		IsParam: true,
		name:    name,
		value:   nil,
	}
}

func NewValue(val interface{}) *ParamOrValue {
	return &ParamOrValue{
		IsParam: false,
		name:    "",
		value:   val,
	}
}

func NewEqParamCondition(param1 *ParamOrValue, param2 *ParamOrValue) Condition {
	ops := make([]interface{}, 0, 2)
	finds := make([]bool, 0, 2)
	params := []*ParamOrValue{param1, param2}
	for _, param := range params {
		finds = append(finds, param.IsParam)
		if param.IsParam {
			ops = append(ops, param.name)
		} else {
			ops = append(ops, param.value)
		}
	}
	return Condition{
		Type: Eq,
		Op:   ops,
		Find: finds,
	}
}

func NewDiffCondition(val1, val2 interface{}) Condition {
	return Condition{
		Type: Diff,
		Op:   []interface{}{val1, val2},
		Find: make([]bool, 2), // all false
	}
}

func NewDiffParamCondition(param1 *ParamOrValue, param2 *ParamOrValue) Condition {
	ops := make([]interface{}, 0, 2)
	finds := make([]bool, 0, 2)
	params := []*ParamOrValue{param1, param2}
	for _, param := range params {
		finds = append(finds, param.IsParam)
		if param.IsParam {
			ops = append(ops, param.name)
		} else {
			ops = append(ops, param.value)
		}
	}
	return Condition{
		Type: Diff,
		Op:   ops,
		Find: finds,
	}
}

func NewGreaterCondition(val1 interface{}, val2 interface{}) Condition {
	b := function.Float{}
	err := b.TypeCheck(val1)
	err2 := b.TypeCheck(val2)
	if err != nil || err2 != nil {
		fmt.Printf("cannot convert values to float: %v, %v\n", val1, val2)
		return NewConstCondition(false)
	}
	return Condition{
		Type: Greater,
		Op:   []interface{}{val1, val2},
		Find: make([]bool, 2), // all false
	}
}

func NewGreaterParamCondition(param1 *ParamOrValue, param2 *ParamOrValue) Condition {
	ops := make([]interface{}, 0, 2)
	finds := make([]bool, 0, 2)
	params := []*ParamOrValue{param1, param2}
	for _, param := range params {
		finds = append(finds, param.IsParam)
		if param.IsParam {
			ops = append(ops, param.name)
		} else {
			ops = append(ops, param.value)
		}
	}
	return Condition{
		Type: Greater,
		Op:   ops,
		Find: finds,
	}
}

func NewSmallerCondition(val1 interface{}, val2 interface{}) Condition {
	b := function.Float{}
	err := b.TypeCheck(val1)
	err2 := b.TypeCheck(val2)
	if err != nil || err2 != nil {
		fmt.Printf("cannot convert values to float: %v, %v\n", val1, val2)
		return NewConstCondition(false)
	}
	return Condition{
		Type: Smaller,
		Op:   []interface{}{val1, val2},
		Find: make([]bool, 2), // all false
	}
}

func NewSmallerParamCondition(param1 *ParamOrValue, param2 *ParamOrValue) Condition {
	ops := make([]interface{}, 0, 2)
	finds := make([]bool, 0, 2)
	params := []*ParamOrValue{param1, param2}
	for _, param := range params {
		finds = append(finds, param.IsParam)
		if param.IsParam {
			ops = append(ops, param.name)
		} else {
			ops = append(ops, param.value)
		}
	}
	return Condition{
		Type: Smaller,
		Op:   ops,
		Find: finds,
	}
}

func NewEmptyCondition(collection []interface{}) Condition {
	return Condition{
		Type: Empty,
		Op:   collection,
		Find: make([]bool, 1), // all false,
	}
}

//func NewEmptyParamCondition(input map[string]interface{}, param1 string) Condition {
//	val1, found := input[param1]
//	if !found {
//		return NewConstCondition(false)
//	}
//	slice, errSlice := utils.ConvertToSlice(val1)
//	if errSlice != nil {
//		return NewConstCondition(false)
//	}
//	return NewEmptyCondition(slice)
//}

type ConditionBuilder struct {
	p      *Predicate
	errors string
}

type RootConditionBuilder struct {
	cb *ConditionBuilder
}

func NewPredicate() *RootConditionBuilder {
	return &RootConditionBuilder{cb: &ConditionBuilder{p: &Predicate{}}}
}

func (rcb *RootConditionBuilder) And(conds ...Condition) *ConditionBuilder {
	and := NewAnd(conds...)
	rcb.cb.p.Root = and
	return rcb.cb
}

func (rcb *RootConditionBuilder) Or(conds ...Condition) *ConditionBuilder {
	or := NewOr(conds...)
	rcb.cb.p.Root = or
	return rcb.cb
}

func (rcb *RootConditionBuilder) Not(cond Condition) *ConditionBuilder {
	not := NewNot(cond)
	rcb.cb.p.Root = not
	return rcb.cb
}

func (cb *ConditionBuilder) Build() Condition {
	return cb.p.Root
}
