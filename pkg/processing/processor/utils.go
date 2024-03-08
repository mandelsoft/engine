package processor

import (
	"slices"
	"strings"

	. "github.com/mandelsoft/engine/pkg/processing/mmids"
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

////////////////////////////////////////////////////////////////////////////////

// OrderedElementSet is a set of elements used for locking
// element graphs. We don't use a plain map here
// to keep the discovery order of elements down the graph.
// The locking is then done in this order
// to provide element change events in a useful order
// for supporting element visualization tools.
type OrderedElementSet = *orderedElementSet

type orderedElementSet struct {
	order []_Element
	elems map[ElementId]_Element
}

func NewOrderedElementSet() OrderedElementSet {
	return &orderedElementSet{
		elems: map[ElementId]_Element{},
	}
}

func (s *orderedElementSet) Size() int {
	return len(s.order)
}

func (s *orderedElementSet) Has(id ElementId) bool {
	_, ok := s.elems[id]
	return ok
}

func (s *orderedElementSet) Add(e _Element) bool {
	if s.Has(e.Id()) {
		return false
	}
	s.order = append(s.order, e)
	s.elems[e.Id()] = e
	return false
}

func (s *orderedElementSet) Order() []_Element {
	return slices.Clone(s.order)
}
