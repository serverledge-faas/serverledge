package function

import (
	"fmt"
	"github.com/grussorusso/serverledge/utils"
	"math"
	"reflect"
	"strconv"
)

type DataTypeEnum interface {
	// VariantName() string
	TypeCheck(val interface{}) error
}

// we do not want a generic Object type: it could lead to bugs and makes harder the conversion

// None represent a nil value
// type None struct{}

// Text represent a string value
type Text struct{}

// Int represent an int value
type Int struct{}

// Float represent a float64 or float32 value
type Float struct{}

// Bool represent a boolean value
type Bool struct{}

// Array represents an array of one of the dataTypes
type Array[D DataTypeEnum] struct {
	DataType D
}

// Option represent a value that can either be a DataType or be nil
// type Option[D DataTypeEnum, N None] struct{}

// TypeCheck checks if val is a string; input val is not always string, so we can use interface as parameter
func (t Text) TypeCheck(val interface{}) error {
	switch val.(type) {
	case string:
		return nil
	default:
		return fmt.Errorf("val should be Text, but is %v", val)
	}
}
func (i Int) TypeCheck(val interface{}) error {
	switch val := val.(type) {
	case int:
		return nil
	case int8:
		return nil
	case int16:
		return nil
	case int32:
		return nil
	case int64:
		return nil
	case float32:
		return nil
	case float64:
		return nil
	case string:
		_, err := strconv.Atoi(val)
		if err == nil {
			return nil
		}
		return fmt.Errorf("val is a string that cannot be cast to an Int: '%s'", val)
	default:
		return fmt.Errorf("val should be Int, but is %v", val)
	}
}

func (b Bool) TypeCheck(val interface{}) error {
	switch v := val.(type) {
	case bool:
		return nil
	case int:
		if v == 1 || v == 0 {
			return nil
		}
		return fmt.Errorf("val is of type int, but cannot be converted to bool")
	case string:
		if v == "false" || v == "False" || v == "true" || v == "True" || v == "1" || v == "0" {
			return nil
		}
		return fmt.Errorf("val is a string that cannot be cast to an Int: '%s'", val.(string))
	default:
		return fmt.Errorf("val should be Bool, but is %v", val)
	}
}

func (f Float) TypeCheck(val interface{}) error {
	switch t := val.(type) {
	case int:
		return nil
	case int8:
		return nil
	case int16:
		return nil
	case int32:
		return nil
	case int64:
		return nil
	case float64:
		return nil
	case float32:
		return nil
	case string:
		_, err := strconv.ParseFloat(t, 32)
		_, err2 := strconv.ParseFloat(t, 64)
		if err == nil || err2 == nil {
			return nil
		}
		return fmt.Errorf("val is a string '%s', but cannot be cast to a Float", t)
	default:
		return fmt.Errorf("val should be Float but is %v", val)
	}
}

// TypeCheck represents an array of one of the dataTypes
func (a Array[D]) TypeCheck(val interface{}) error {
	switch reflect.TypeOf(val).Kind() {
	case reflect.Slice:
		// convert interface{} to []interface{}
		genericSlice, errNotSlice := utils.ConvertToSlice(val)
		if errNotSlice != nil {
			return errNotSlice
		}

		typeError := ""
		for i, t := range genericSlice {
			err := a.DataType.TypeCheck(t)
			if err != nil {
				typeError += fmt.Sprintf("\ntype-error: element %d of slice has wrong type", i)
				break
			}
		}
		if typeError != "" {
			return fmt.Errorf("%s", typeError)
		}
		return nil
	default: // if we have only a single type, we type check that it is of the same type of Array's type parameter
		typeElem := reflect.TypeOf(val).Name()
		// fmt.Printf("name of type: %s\n", typeElem)
		err := a.DataType.TypeCheck(val)
		if err != nil {
			return fmt.Errorf("val should be a slice, but is %v of type %s\n", val, typeElem)
		}
		return nil
	}
}

func (t Text) Convert(val interface{}) (string, error) {
	switch t := val.(type) {
	case string:
		return t, nil
	case int:
		return fmt.Sprintf("%d", t), nil
	case int8:
		return fmt.Sprintf("%d", t), nil
	case int16:
		return fmt.Sprintf("%d", t), nil
	case int32:
		return fmt.Sprintf("%d", t), nil
	case int64:
		return fmt.Sprintf("%d", t), nil
	case float32:
		return fmt.Sprintf("%f", t), nil
	case float64:
		return fmt.Sprintf("%f", t), nil
	case bool:
		return fmt.Sprintf("%v", t), nil
	default:
		return "", fmt.Errorf("val should be Text, but is %v", val)
	}
}

func (i Int) Convert(val interface{}) (int, error) {
	switch t := val.(type) {
	case int:
		return t, nil
	case int8:
		return int(t), nil
	case int16:
		return int(t), nil
	case int32:
		return int(t), nil
	case int64:
		return int(t), nil
	case string:
		val, err := strconv.Atoi(val.(string))
		if err == nil {
			return val, nil
		}
		return 0, fmt.Errorf("val is a string '%s', but cannot be cast to an Int", t)
	default:
		return 0, fmt.Errorf("val should be Int, but is %v", val)
	}
}
func (b Bool) Convert(val interface{}) (bool, error) {
	switch t := val.(type) {
	case bool:
		return t, nil
	case int:
		if t != 0 {
			return true, nil
		} else {
			return false, nil
		}
	case string:
		if t == "true" || t == "True" || t == "1" {
			return true, nil
		} else if t == "false" || t == "False" || t == "0" {
			return false, nil
		} else {
			return false, fmt.Errorf("val is of type string, but cannot be converted to bool")
		}
	default:
		return false, fmt.Errorf("value %v does not represent a Bool", val)
	}
}

func (f Float) Convert(val interface{}) (float64, error) {
	switch t := val.(type) {
	case int:
		return float64(t), nil
	case int8:
		return float64(t), nil
	case int16:
		return float64(t), nil
	case int32:
		return float64(t), nil
	case int64:
		return float64(t), nil
	case float64:
		return t, nil
	case float32:
		return float64(t), nil
	case bool:
		if t {
			return 1.0, nil
		}
		return 0.0, nil
	case string:
		val64, err64 := strconv.ParseFloat(t, 64)
		if err64 == nil {
			return val64, nil
		}
		val32, err32 := strconv.ParseFloat(t, 32)
		if err32 == nil {
			return val32, nil
		}
		return math.NaN(), fmt.Errorf("val is a string '%s', but cannot be cast to a Float", t)
	default:
		return math.NaN(), fmt.Errorf("val should be Float but is %v", val)
	}
}
