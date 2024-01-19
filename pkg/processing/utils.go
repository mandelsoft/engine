package processing

import (
	"strings"
)

func ParentNamespace(ns string) string {
	i := strings.LastIndex(ns, "/")
	if i < 0 {
		return ""
	}
	return ns[:i]
}

func NamespaceName(ns string) string {
	i := strings.LastIndex(ns, "/")
	if i < 0 {
		return ns
	}
	return ns[i+1:]
}
