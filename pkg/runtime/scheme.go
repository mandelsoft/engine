package runtime

import (
	"fmt"
	"reflect"
	"sort"
	"sync"

	"sigs.k8s.io/yaml"
)

type TypeAccessor interface {
	GetType() string
}

type Object interface {
	TypeAccessor
	SetType(string)
}

type ObjectMeta struct {
	Type string `json:"type"`
}

var _ Object = (*ObjectMeta)(nil)

func (o *ObjectMeta) GetType() string {
	return o.Type
}

func (o *ObjectMeta) SetType(t string) {
	o.Type = t
}

type Scheme[E Object] interface {
	Register(name string, proto E) error

	Encoding[E]
}

type scheme[E Object] struct {
	lock  sync.Mutex
	types map[string]reflect.Type
}

var _ Scheme[Object] = (*scheme[Object])(nil)

func NewYAMLScheme[E Object]() Scheme[E] {
	return &scheme[E]{types: map[string]reflect.Type{}}
}

func (s *scheme[E]) Register(name string, proto E) error {
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

func (s *scheme[E]) HasType(t string) bool {
	s.lock.Lock()
	defer s.lock.Unlock()
	return s.types[t] != nil
}

func (s *scheme[E]) CreateObject(typ string) (E, error) {
	var _nil E

	s.lock.Lock()
	defer s.lock.Unlock()

	t := s.types[typ]
	if t == nil {
		return _nil, fmt.Errorf("unknown object type %q", typ)
	}

	o := reflect.New(t).Interface().(E)
	o.SetType(typ)
	return o, nil
}

func (s *scheme[E]) TypeNames() []string {
	var names []string

	s.lock.Lock()
	defer s.lock.Unlock()

	for n := range s.types {
		names = append(names, n)
	}
	sort.Strings(names)
	return names
}

func (s *scheme[E]) Decode(data []byte) (E, error) {
	var ty ObjectMeta
	var _nil E

	err := yaml.Unmarshal(data, &ty)
	if err != nil {
		return _nil, err
	}

	s.lock.Lock()
	defer s.lock.Unlock()

	t := s.types[ty.Type]
	if t == nil {
		return _nil, fmt.Errorf("unknown object type %q", ty.Type)
	}

	v := reflect.New(t).Interface()

	err = yaml.Unmarshal(data, v)
	if err != nil {
		return _nil, err
	}
	return v.(E), nil
}

type ElementType[P any] interface {
	Object
	*P
}

func Register[T any, P ElementType[T], E Object](s Scheme[E], name string) error {
	var proto T

	p, ok := (any(&proto)).(E)
	if !ok {
		return fmt.Errorf("*%s does not implement scheme interface %s", TypeOf[T](), TypeOf[E]())
	}
	return s.Register(name, p)
}

func MustRegister[T any, P ElementType[T], E Object](s Scheme[E], name string) {
	err := Register[T, P, E](s, name)
	if err != nil {
		panic(err)
	}
}

// test

func t() {
	var s Scheme[Object]

	Register[ObjectMeta](s, "test")
}
