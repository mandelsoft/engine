package processor

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

func NamespaceId(ns string) (string, string) {
	i := strings.LastIndex(ns, "/")
	if i < 0 {
		return "", ns
	}
	return ns[:i], ns[i+1:]
}

type description interface {
	Description() string
}
type getdescription interface {
	GetDescription() string
}
type getversion interface {
	GetVersion() string
}

func DescribeObject(o any) string {
	if d, ok := o.(getdescription); ok {
		return d.GetDescription()
	}
	if d, ok := o.(description); ok {
		return d.Description()
	}
	if d, ok := o.(getversion); ok {
		return d.GetVersion()
	}
	return "<no description>"
}
