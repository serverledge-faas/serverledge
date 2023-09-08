package function

import (
	"fmt"
	"reflect"
	"strconv"
)

const (
	INT               = "Int"
	FLOAT             = "Float"
	TEXT              = "Text"
	BOOL              = "Bool"
	ARRAY_INT         = "ArrayInt"
	ARRAY_FLOAT       = "ArrayFloat"
	ARRAY_BOOL        = "ArrayBool"
	ARRAY_TEXT        = "ArrayText"
	ARRAY_ARRAY_INT   = "ArrayArrayInt"
	ARRAY_ARRAY_FLOAT = "ArrayArrayFloat"
)

// InputDef can be used to represent and check an input type with its name in the map
type InputDef struct {
	Name string // the name of the input parameter, also in the map key
	Type string // the type of the input parameter
}

// CheckInput evaluates all given inputs and if there is no input that type-checks, with the given name and the current type, returns an error
func (i InputDef) CheckInput(inputMap map[string]interface{}) error {

	val, exists := inputMap[i.Name]
	if !exists {
		return fmt.Errorf("no input parameter with name '%s' and type '%s' exists", i.Name, i.Type)
	}

	t := StringToDataType(i.Type)
	if t == nil {
		return fmt.Errorf("data type is too complex. Available types are Int, Text, Float, Bool, ArrayInt, ArrayText, ArrayFloat, ArrayBool, ArrayArrayInt, ArrayArrayFloat")
	}

	return StringToDataType(i.Type).TypeCheck(val)
}

func (i InputDef) FindEntryThatTypeChecks(outputMap map[string]interface{}) (string, bool) {
	for k, v := range outputMap {

		t := StringToDataType(i.Type)
		if t == nil {
			return "", false
		}

		err := t.TypeCheck(v)
		if err == nil {
			return k, true
		}
	}
	return "", false
}

// OutputDef can be used to represent and check an output type with its name in the map
type OutputDef struct {
	Name string // the name of the map key for the output parameter
	Type string // the type of the output parameter
}

// CheckInput evaluates all given outputs and if there is no output that type-checks, with the given name and the current type, returns an error
func (o OutputDef) CheckOutput(inputMap map[string]interface{}) error {
	val, exists := inputMap[o.Name]
	if !exists {
		return fmt.Errorf("no output parameter with name '%s' and type '%s' exists", o.Name, reflect.TypeOf(o.Type).Name())
	}
	t := StringToDataType(o.Type)
	if t != nil {
		return t.TypeCheck(val)
	}
	return nil
}

func (o OutputDef) TryParse(result string) (interface{}, error) {
	t := StringToDataType(o.Type)
	if t == nil {
		return nil, fmt.Errorf("type %s is not a compatible type", datatypeToString(t))
	}
	switch t.(type) {
	case Int:
		return strconv.Atoi(result)
	case Text:
		return result, nil
	case Bool:
		return strconv.ParseBool(result)
	case Float:
		return strconv.ParseFloat(result, 64)
	//case Array[DataTypeEnum]:
	//	arr := make([]interface{}, 0)
	//	dType := o.Type.(Array[DataTypeEnum]).DataType.TryParse()

	default:
		return result, nil
	}
}

// Signature can be used to check all inputs/outputs that are passed to/returned by functions
type Signature struct {
	Inputs  []*InputDef  // if empty, the function doesn't use parameters
	Outputs []*OutputDef // if empty, the function doesn't return anything
}

// SignatureBuilder can be used to dynamically build a signature
type SignatureBuilder struct {
	signature Signature
}

func NewSignature() SignatureBuilder {
	return SignatureBuilder{signature: Signature{
		Inputs:  make([]*InputDef, 0),
		Outputs: make([]*OutputDef, 0),
	}}
}

func (s SignatureBuilder) addInputDef(def *InputDef) SignatureBuilder {
	s.signature.Inputs = append(s.signature.Inputs, def)
	return s
}

func (s SignatureBuilder) addOutputDef(def *OutputDef) SignatureBuilder {
	s.signature.Outputs = append(s.signature.Outputs, def)
	return s
}

func (s SignatureBuilder) AddInput(name string, dataType DataTypeEnum) SignatureBuilder {
	str := datatypeToString(dataType)
	def := InputDef{
		Name: name,
		Type: str,
	}
	return s.addInputDef(&def)
}

func (s SignatureBuilder) AddOutput(name string, dataType DataTypeEnum) SignatureBuilder {
	def := OutputDef{
		Name: name,
		Type: datatypeToString(dataType),
	}
	return s.addOutputDef(&def)
}

func (s SignatureBuilder) Build() *Signature {
	return &s.signature
}

func (s *Signature) CheckAllInputs(inputMap map[string]interface{}) error {
	errors := ""
	// number of inputs should be the same, but sometimes we need the input for the subsequent functions, but not for the current one
	//if len(inputMap) != len(s.inputs) {
	//	errors += fmt.Sprintf("type-error: there are %d inputs, but should have been %d\n", len(inputMap), len(s.inputs))
	//}
	// type of inputs in the signature
	for _, def := range s.Inputs {
		err := def.CheckInput(inputMap)
		if err != nil {
			errors += fmt.Sprintf("type-error: %v", err)
		}
	}
	if errors != "" {
		return fmt.Errorf("%s", errors)
	}
	return nil
}

func (s *Signature) CheckAllOutputs(outputMap map[string]interface{}) error {
	errors := ""
	// number of outputs: we should not check it if we are taking with use more input than necessary for this function
	//if len(outputMap) != len(s.outputs) {
	//	errors += fmt.Sprintf("type-error: there are %d outputs, but should have been %d\n", len(outputMap), len(s.outputs))
	//}
	// type of outputs in the signature
	for _, def := range s.Outputs {
		err := def.CheckOutput(outputMap)
		if err != nil {
			errors += fmt.Sprintf("type-error: %v", err)
		}
	}
	if errors != "" {
		return fmt.Errorf("%s", errors)
	}
	return nil
}

func (s *Signature) GetInputs() []*InputDef {
	return s.Inputs
}

func (s *Signature) GetOutputs() []*OutputDef {
	return s.Outputs
}

func StringToDataType(t string) DataTypeEnum {
	switch t {
	case INT:
		return Int{}
	case FLOAT:
		return Float{}
	case BOOL:
		return Bool{}
	case TEXT:
		return Text{}
	case ARRAY_INT:
		return Array[Int]{DataType: Int{}}
	case ARRAY_FLOAT:
		return Array[Float]{DataType: Float{}}
	case ARRAY_BOOL:
		return Array[Bool]{DataType: Bool{}}
	case ARRAY_TEXT:
		return Array[Text]{DataType: Text{}}
	case ARRAY_ARRAY_INT:
		return Array[Array[Int]]{DataType: Array[Int]{DataType: Int{}}}
	case ARRAY_ARRAY_FLOAT:
		return Array[Array[Float]]{DataType: Array[Float]{DataType: Float{}}}
	default:
		return nil
	}
}

func datatypeToString(dataType DataTypeEnum) string {
	switch dataType.(type) {
	case Int:
		return INT
	case Float:
		return FLOAT
	case Bool:
		return BOOL
	case Text:
		return TEXT
	case Array[Int]:
		return ARRAY_INT
	case Array[Float]:
		return ARRAY_FLOAT
	case Array[Bool]:
		return ARRAY_BOOL
	case Array[Text]:
		return ARRAY_TEXT
	case Array[Array[Int]]:
		return ARRAY_ARRAY_INT
	case Array[Array[Float]]:
		return ARRAY_ARRAY_FLOAT
	default:
		return ""
	}
}
