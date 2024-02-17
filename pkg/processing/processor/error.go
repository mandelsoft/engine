package processor

import (
	"errors"
)

type nonTemporaryError struct {
	error
}

func NonTemporaryError(err error) error {
	return &nonTemporaryError{err}
}

func (e *nonTemporaryError) Unwrap() error {
	return e.error
}

var protoNonTemp = &nonTemporaryError{}

func IsNonTemporary(err error) bool {
	return errors.Is(err, protoNonTemp)
}
