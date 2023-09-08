package test

import (
	"fmt"
	"github.com/grussorusso/serverledge/internal/function"
	u "github.com/grussorusso/serverledge/utils"

	"testing"
)

// DataTypeEnum Text test
func TestText(t *testing.T) {
	tt := function.Text{}

	err := tt.TypeCheck("text")
	u.AssertNil(t, err)

	err2 := tt.TypeCheck(5)
	u.AssertNonNil(t, err2)
	fmt.Println(err2.Error())
}

// DataTypeEnum Int test
func TestInt(t *testing.T) {
	// Int represent an int value
	i := function.Int{}

	err := i.TypeCheck(4)
	u.AssertNil(t, err)

	err2 := i.TypeCheck("5")
	u.AssertNil(t, err2)

	err3 := i.TypeCheck("0103.1")
	u.AssertNonNil(t, err3)
	fmt.Println(err3.Error())

}

// DataTypeEnum Float test
func TestFloat(t *testing.T) {
	f := function.Float{}

	err := f.TypeCheck(2.5)
	u.AssertNil(t, err)

	err2 := f.TypeCheck("2.5")
	u.AssertNil(t, err2)

	err3 := f.TypeCheck(1)
	u.AssertNil(t, err3)

	err4 := f.TypeCheck("pizza")
	u.AssertNonNil(t, err4)

}

func TestBoolean(t *testing.T) {
	f := function.Bool{}

	err := f.TypeCheck(true)
	u.AssertNil(t, err)

	err2 := f.TypeCheck("false")
	u.AssertNil(t, err2)

	err3 := f.TypeCheck(1)
	u.AssertNil(t, err3)

	err4 := f.TypeCheck("0")
	u.AssertNil(t, err4)

	err5 := f.TypeCheck("fake")
	u.AssertNonNil(t, err5)

}

// DataTypeEnum Array test
func TestArray(t *testing.T) {
	a := function.Array[function.Int]{}

	err := a.TypeCheck([]int{1, 2, 3, 4})
	u.AssertNil(t, err)

	err2 := a.TypeCheck([]int{})
	u.AssertNil(t, err2)
	// if the slice is empty, we do not care of the type
	err3 := a.TypeCheck([]string{})
	u.AssertNil(t, err3)

	err4 := a.TypeCheck([]string{"a", "b", "c"})
	u.AssertNonNil(t, err4)

	err5 := a.TypeCheck(999)
	u.AssertNil(t, err5)
}
