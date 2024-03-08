package filesystem

import (
	"fmt"
	"regexp"

	"github.com/mandelsoft/engine/pkg/database"
)

func Path(o database.ObjectId) string {
	return fmt.Sprintf("%s/%s/%s.yaml", o.GetType(), o.GetNamespace(), o.GetName())
}

var nameExp = regexp.MustCompile("^[a-zA-Z][a-zA-Z0-9-_]*$")
var nsExp = regexp.MustCompile("^[a-zA-Z][a-zA-Z0-9-_]*(/[a-zA-Z][a-zA-Z0-9-_]*)*$")

func CheckName(n string) bool {
	return nameExp.MatchString(n)
}

func CheckType(n string) bool {
	return nameExp.MatchString(n)
}

func CheckNamespace(n string) bool {
	if n == "" {
		return true
	}
	return nsExp.MatchString(n)
}

func CheckId(id database.ObjectId) bool {
	return CheckNamespace(id.GetNamespace()) && CheckName(id.GetName()) && CheckType(id.GetType())
}
