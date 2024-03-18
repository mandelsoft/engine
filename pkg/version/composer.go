package version

import (
	"fmt"
	"strings"

	"github.com/mandelsoft/goutils/general"
)

func Compose(n Node, nested ...string) string {
	if len(nested) == 0 {
		return GetVersionedName(n)
	}
	return fmt.Sprintf("%s(%s)", GetVersionedName(n), strings.Join(nested, ","))
}

var Composed = ComposeFunc(Compose)

type Composer interface {
	Compose(n Node, nested ...string) string
}

type ComposeFunc func(n Node, nested ...string) string

func (c ComposeFunc) Compose(n Node, nested ...string) string {
	return c(n, nested...)
}

func Hash(n Node, nested ...string) string {
	return general.HashData(Compose(n, nested...))
}

var Hashed = ComposeFunc(Hash)
