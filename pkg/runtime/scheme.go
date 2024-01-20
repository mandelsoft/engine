package runtime

import (
	"sigs.k8s.io/yaml"
)

// Encoding provides object decoding for scheme types.
type Encoding[T Object] interface {
	SchemeTypes[T]

	Decode(data []byte) (T, error)
}

// Scheme is an encoding with registration.
type Scheme[E Object] interface {
	Encoding[E]
	TypeScheme[E]
}

type scheme[E Object] struct {
	types[E]
}

var _ Scheme[Object] = (*scheme[Object])(nil)

func NewYAMLScheme[E Object]() Scheme[E] {
	return &scheme[E]{*NewTypeScheme[E]()}
}

func (s *scheme[E]) Decode(data []byte) (E, error) {
	var ty ObjectMeta
	var _nil E

	err := yaml.Unmarshal(data, &ty)
	if err != nil {
		return _nil, err
	}

	v, err := s.CreateObject(ty.Type)
	if err != nil {
		return _nil, err
	}

	err = yaml.Unmarshal(data, v)
	if err != nil {
		return _nil, err
	}
	return v, nil
}

// test

func t() {
	var s Scheme[Object]

	s.Register("test1", &ObjectMeta{})

	Register[ObjectMeta](TypeScheme[Object](s), "test") // Goland: requires interface conversion
	// Register[ObjectMeta](s, "test") // Goland: requires interface conversion
}
