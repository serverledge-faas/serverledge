package types

// TODO: move out of this package
type Comparable interface {
	Equals(cmp Comparable) bool
}
