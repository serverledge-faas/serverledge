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
	VOID              = "Void"
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

	dType, err := StringToDataType(i.Type)
	if err != nil {
		return fmt.Errorf("data type is too complex. Available types are Int, Text, Float, Bool, ArrayInt, ArrayText, ArrayFloat, ArrayBool, ArrayArrayInt, ArrayArrayFloat")
	}
	return dType.TypeCheck(val)
}

func (i InputDef) FindEntryThatTypeChecks(outputMap map[string]interface{}) (string, bool) {
	for k, v := range outputMap {

		t, err := StringToDataType(i.Type)
		if err != nil {
			return "", false
		}
		if t == nil {
			return "", false
		}

		err = t.TypeCheck(v)
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

// CheckOutput evaluates all given outputs and if there is no output that type-checks, with the given name and the current type, returns an error
func (o OutputDef) CheckOutput(inputMap map[string]interface{}) error {
	val, exists := inputMap[o.Name]
	if !exists {
		return fmt.Errorf("no output parameter with name '%s' and type '%s' exists", o.Name, reflect.TypeOf(o.Type).Name())
	}
	t, err := StringToDataType(o.Type)
	if err != nil {
		return err
	}
	if t != nil {
		return t.TypeCheck(val)
	}
	return nil
}

func (o OutputDef) TryParse(result string) (interface{}, error) {
	t, err := StringToDataType(o.Type)
	if err != nil {
		return nil, fmt.Errorf("type %s is not a compatible type", o.Type)
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
	default:
		return result, nil
	}
}

// Signature can be used to check all inputs/outputs that are passed to/returned by functions
type Signature struct {
	Inputs  []*InputDef  // if empty, the function doesn't use parameters
	Outputs []*OutputDef // if empty, the function doesn't return anything
}

func (s *Signature) String() string {
	str := "Inputs: ["
	for i, input := range s.Inputs {
		str += fmt.Sprintf("%s:%s", input.Name, input.Type)
		if i != len(s.Inputs)-1 {
			str += ", "
		}
	}
	str += "]"
	str += " Outputs: ["
	for i, output := range s.Outputs {
		str += fmt.Sprintf("%s:%s", output.Name, output.Type)
		if i != len(s.Outputs)-1 {
			str += ", "
		}
	}
	str += "]"

	return str
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
	if len(s.signature.Inputs) == 0 {
		s.AddInput("", Void{})
	}
	if len(s.signature.Outputs) == 0 {
		s.AddOutput("", Void{})
	}
	return &s.signature
}

// CheckOrMatchInputs performs name and type checking between inputMap and the signature.
// If the signature has a single input that type-checks with inputMap, the name of
// the parameter in inputMap is updated to match with the signature.
func (s *Signature) CheckOrMatchInputs(inputMap map[string]interface{}) error {
	errors := ""
	if len(inputMap) < len(s.Inputs) {
		errors += fmt.Sprintf("type-error: there are %d inputs, but should have been %d\n", len(inputMap), len(s.Inputs))
	}

	for _, def := range s.Inputs {
		err := def.CheckInput(inputMap)

		if err != nil && len(s.Inputs) == 1 {
			key, ok := def.FindEntryThatTypeChecks(inputMap)
			if ok {
				val := inputMap[key]
				delete(inputMap, key)
				inputMap[def.Name] = val
				err = nil
			} else {
				err = fmt.Errorf("no output entry input-checks with the next function")
			}
		}

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

	if len(outputMap) < len(s.Outputs) {
		errors += fmt.Sprintf("type-error: there are %d outputs, but should have been %d\n", len(outputMap), len(s.Outputs))
	}

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

func StringToDataType(t string) (DataTypeEnum, error) {
	switch t {
	case INT:
		return Int{}, nil
	case FLOAT:
		return Float{}, nil
	case BOOL:
		return Bool{}, nil
	case TEXT:
		return Text{}, nil
	case ARRAY_INT:
		return Array[Int]{DataType: Int{}}, nil
	case ARRAY_FLOAT:
		return Array[Float]{DataType: Float{}}, nil
	case ARRAY_BOOL:
		return Array[Bool]{DataType: Bool{}}, nil
	case ARRAY_TEXT:
		return Array[Text]{DataType: Text{}}, nil
	case ARRAY_ARRAY_INT:
		return Array[Array[Int]]{DataType: Array[Int]{DataType: Int{}}}, nil
	case ARRAY_ARRAY_FLOAT:
		return Array[Array[Float]]{DataType: Array[Float]{DataType: Float{}}}, nil
	case VOID:
		return Void{}, nil
	default:
		return nil, fmt.Errorf("invalid datatype: %s", t)
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
	case Void:
		return VOID
	default:
		return ""
	}
}

// SignatureInference is a best-effort function that tries to infer signature from a function without a defined signature. Maybe we do not need it.
func SignatureInference(params map[string]interface{}) *Signature {
	signatureBuilder := NewSignature()

	for k, v := range params {
		typeList := []DataTypeEnum{
			Float{},
			Int{},
			Bool{},
			Text{},
			Array[Float]{},
			Array[Int]{},
			Array[Bool]{},
			Array[Text]{},
			Void{},
		}
		for _, t := range typeList {
			if t.TypeCheck(v) == nil {
				signatureBuilder.AddInput(k, t)
				break
			}
		}
	}

	return signatureBuilder.AddOutput("result", Text{}).Build()
}
