package utils

// ReturnNonNilErr returns the first non-nil. If all errors are nil, returns nil.
func ReturnNonNilErr(errs ...error) error {
	for _, e := range errs {
		if e != nil {
			return e
		}
	}
	return nil
}
