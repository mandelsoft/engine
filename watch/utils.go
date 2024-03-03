package watch

import (
	"errors"
	"io"
	"strings"
)

// IsErrClosed check for an unexported error.
func IsErrClosed(err error) bool {
	if err == nil {
		return false
	}
	return errors.Is(err, io.EOF) || strings.Contains(err.Error(), "use of closed network connection")
}
