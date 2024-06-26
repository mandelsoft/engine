package runtime

import (
	"fmt"
	"reflect"
	"sort"
	"sync"

	"github.com/mandelsoft/goutils/generics"
)

type Initializer[T Object] func(o T)

// InitializedObject is the interface of an [Object],
// which provides an initialization method as part of
// its implementation. If present it is called by the scheme
// object creation prior to calling the explicitly
// specified [Initializer] functions to provide an
// initialized object
type InitializedObject interface {
	Initialize() error
}

// SchemeTypes is a set of type definitions
// mapping type names to Go types.
// This mapping is used to provide a simple
// object creation by type name.
type SchemeTypes[T Object] interface {
	TypeNames() []string
	HasType(t string) bool
	CreateObject(typ string, init ...Initializer[T]) (T, error)
}

// TypeScheme is a set types with a registration possibility.
type TypeScheme[T Object] interface {
	SchemeTypes[T]

	Register(name string, proto T) error
}

type types[E Object] struct {
	lock  sync.Mutex
	types map[string]reflect.Type
}

var _ SchemeTypes[Object] = (*types[Object])(nil)

func NewTypeScheme[E Object]() *types[E] {
	return &types[E]{types: map[string]reflect.Type{}}
}

func (s *types[E]) Register(name string, proto E) error {
	s.lock.Lock()
	defer s.lock.Unlock()

	t := reflect.TypeOf(proto)
	if t.Kind() != reflect.Pointer {
		return fmt.Errorf("proto type for %s must be pointer", name)
	}
	t = t.Elem()
	if t.Kind() != reflect.Struct {
		return fmt.Errorf("proto type for %s must be pointer to struct", name)
	}

	s.types[name] = t
	return nil
}

func (s *types[E]) HasType(t string) bool {
	s.lock.Lock()
	defer s.lock.Unlock()
	return s.types[t] != nil
}

func (s *types[E]) CreateObject(typ string, init ...Initializer[E]) (E, error) {
	var _nil E

	s.lock.Lock()
	defer s.lock.Unlock()

	t := s.types[typ]
	if t == nil {
		return _nil, fmt.Errorf("unknown object type %q", typ)
	}

	v := reflect.New(t)
	o := v.Interface().(E)

	if i, ok := generics.TryCast[InitializedObject](o); ok {
		err := i.Initialize()
		if err != nil {
			return _nil, err
		}
	}
	for _, i := range init {
		i(o)
	}
	o.SetType(typ)
	return o, nil
}

func (s *types[E]) TypeNames() []string {
	var names []string

	s.lock.Lock()
	defer s.lock.Unlock()

	for n := range s.types {
		names = append(names, n)
	}
	sort.Strings(names)
	return names
}

type ElementType[P any] interface {
	Object
	*P
}

func Register[T any, P ElementType[T], E Object](s TypeScheme[E], name string) error {
	var proto T

	p, ok := (any(&proto)).(E)
	if !ok {
		return fmt.Errorf("*%s does not implement scheme interface %s", generics.TypeOf[T](), generics.TypeOf[E]())
	}
	return s.Register(name, p)
}

func MustRegister[T any, P ElementType[T], E Object](s TypeScheme[E], name string) {
	err := Register[T, P, E](s, name)
	if err != nil {
		panic(err)
	}
}
