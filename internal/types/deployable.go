package types

type Comparable interface {
	Equals(cmp Comparable) bool
}

type Savable interface {
	SaveToEtcd() error
}

type Invokable interface {
	Invoke() (map[string]interface{}, error)
}

type Removable interface {
	Delete() error
}

type Pollable interface {
	Poll() interface{}
}

// Deployable is a function.Function or a FunctionComposition
type Deployable interface {
	Comparable
	Savable
	Removable
	// Invokable // uncomment with correct signature for function.Function and FunctionComposition
	// Pollable // uncomment with correct signature for function.Function and FunctionComposition
}

var NodeDoneChan = make(chan string)
