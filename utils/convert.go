package utils

import (
	"fmt"
	"reflect"
)

func ConvertToSlice(v interface{}) ([]interface{}, error) {
	var out []interface{}
	rv := reflect.ValueOf(v)
	if rv.Kind() == reflect.Slice {
		for i := 0; i < rv.Len(); i++ {
			out = append(out, rv.Index(i).Interface())
		}
	} else {
		return nil, fmt.Errorf("cannot convert interface to interface slice")
	}
	return out, nil
}

func ConvertToSpecificSlice[T any](slice []interface{}) ([]T, error) {
	out := make([]T, 0, len(slice))
	for _, value := range slice {
		castedValue, ok := value.(T)
		if !ok {
			return nil, fmt.Errorf("failed to convert to generic type")
		}
		out = append(out, castedValue)
	}

	return out, nil
}

func ConvertInterfaceToSpecificSlice[T any](val interface{}) ([]T, error) {
	slice, err := ConvertToSlice(val)
	if err != nil {
		return nil, err
	}
	specificSlice, err := ConvertToSpecificSlice[T](slice)
	if err != nil {
		return nil, err
	}
	return specificSlice, nil
}
