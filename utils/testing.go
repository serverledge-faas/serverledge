package utils

import (
	"golang.org/x/exp/maps"
	"golang.org/x/exp/slices"
	"testing"
)

func AssertEquals[T comparable](t *testing.T, expected T, result T) {
	if expected != result {
		t.Logf("%s is failed. Got '%v', expected '%v'", t.Name(), result, expected)
		t.FailNow()
	}
}

func AssertEqualsMsg[T comparable](t *testing.T, expected T, result T, msg string) {
	if expected != result {
		t.Logf("%s is failed; %s - Got '%v', expected '%v'", t.Name(), msg, result, expected)
		t.FailNow()
	}
}

func AssertSliceEquals[T comparable](t *testing.T, expected []T, result []T) {
	if equal := slices.Equal(expected, result); !equal {
		t.Logf("%s is failed Got '%v', expected '%v'", t.Name(), result, expected)
		t.FailNow()
	}
}

func AssertSliceEqualsMsg[T comparable](t *testing.T, expected []T, result []T, msg string) {
	if equal := slices.Equal(expected, result); !equal {
		t.Logf("%s is failed; %s - Got '%v', expected '%v'", t.Name(), msg, result, expected)
		t.FailNow()
	}
}

func AssertMapEquals[K comparable, V comparable](t *testing.T, expectedMap map[K]V, resultMap map[K]interface{}) {
	typedMap := make(map[K]V)
	for k, v := range resultMap {
		typedMap[k] = v.(V)
	}
	if equal := maps.Equal(expectedMap, typedMap); !equal {
		t.Logf("%s is failed. Got '%v', expected '%v'", t.Name(), resultMap, expectedMap)
		t.FailNow()
	}
}

func AssertNil(t *testing.T, result interface{}) {
	if nil != result {
		t.Logf("%s is failed. Got '%v', expected nil", t.Name(), result)
		t.FailNow()
	}
}

func AssertNilMsg(t *testing.T, result interface{}, msg string) {
	if nil != result {
		t.Logf("%s is failed; %s - Got '%v', expected nil", t.Name(), result, msg)
		t.FailNow()
	}
}

func AssertNonNil(t *testing.T, result interface{}) {
	if nil == result {
		t.Logf("%s is failed. Got '%v', expected non-nil", t.Name(), result)
		t.FailNow()
	}
}

func AssertNonNilMsg(t *testing.T, result interface{}, msg string) {
	if nil == result {
		t.Logf("%s is failed; %s - Got '%v', expected non-nil", t.Name(), result, msg)
		t.FailNow()
	}
}

// AssertNotEmptySlice asserts that a slice is not empty. Notice: here we use generics. Only works for go 1.19+
func AssertNotEmptySlice[A any](t *testing.T, slice []*A) {
	if slice == nil {
		t.Logf("%s is failed. The slice is nil,", t.Name())
		t.FailNow()
	}
	if len(slice) == 0 {
		t.Logf("%s is failed. The slice is empty,", t.Name())
		t.FailNow()
	}
}

func AssertEmptySlice[T any](t *testing.T, slice []T) {
	if slice == nil {
		t.Logf("%s is failed. The slice is nil,", t.Name())
		t.FailNow()
	}
	if len(slice) != 0 {
		t.Logf("%s is failed. The slice is NOT empty,", t.Name())
		t.FailNow()
	}
}

func AssertTrue(t *testing.T, isTrue bool) {
	if !isTrue {
		t.Logf("%s is failed. Got false", t.Name())
		t.FailNow()
	}
}

func AssertTrueMsg(t *testing.T, isTrue bool, msg string) {
	if !isTrue {
		t.Logf("%s is false - %s", t.Name(), msg)
		t.FailNow()
	}
}

func AssertFalse(t *testing.T, isTrue bool) {
	if isTrue {
		t.Logf("%s is failed. Got true", t.Name())
		t.FailNow()
	}
}

func AssertFalseMsg(t *testing.T, isTrue bool, msg string) {
	if isTrue {
		t.Logf("%s is true - %s", t.Name(), msg)
		t.FailNow()
	}
}
