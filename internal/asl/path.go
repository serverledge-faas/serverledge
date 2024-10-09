package asl

import (
	"fmt"
	"reflect"
	"strings"
)

// Path is a string beginning with "$", used to identify components in a JSON text, in JSONPath format.
// Used to navigate an input parameter for a state
type Path string

// NewReferencePath creates a new Reference Path, that starts with a $ character, separated by "." characters and
// that does not contain the following characters '@' ',' ':' '?'. Used to define input or output parameters.
func NewReferencePath(s string) (Path, error) {
	if s == "" || s == "$" {
		return Path(s), nil
	}
	if !strings.HasPrefix(s, "$.") {
		s = "$." + s
	}
	if strings.Contains(s, "@") || strings.Contains(s, ",") || strings.Contains(s, ":") || strings.Contains(s, "?") {
		return "", fmt.Errorf("A reference path should not contain any of the following characters: '@' ',' ':' '?' ")
	}
	return Path(s), nil
}

// IsReferencePath checks whether the input is a valid reference path string or not (e.g. starts with '$')
func IsReferencePath(valpar interface{}) bool {
	if reflect.TypeOf(valpar).Kind() == reflect.String {
		s, ok := valpar.(string)
		if !ok {
			fmt.Printf("this should never happen: parameter has kind string, but is not a string")
			return false
		}
		return s == "$" || (strings.HasPrefix(s, "$.") && len(s) > 2)
	}
	return false
}

// RemoveDollar removes the leading '$.' from the reference path. It leaves subsequent '.' in it.
func RemoveDollar(s string) string {
	if s == "" || s == "$" {
		return ""
	} else if strings.HasPrefix(s, "$.") {
		return s[2:]
	}
	return s
}
