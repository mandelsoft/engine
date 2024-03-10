package runtime

import (
	"sigs.k8s.io/yaml"
)

type TypeExtractor func(data []byte) (string, error)

type accessorPointer[P any] interface {
	TypeAccessor
	*P
}

func TypeExtractorFor[O any, P accessorPointer[O]]() TypeExtractor {
	return func(data []byte) (string, error) {
		var meta O

		err := yaml.Unmarshal(data, &meta)
		if err != nil {
			return "", err
		}
		return P(&meta).GetType(), nil
	}
}

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
	typeExtractor TypeExtractor
}

var _ Scheme[Object] = (*scheme[Object])(nil)

func NewYAMLScheme[E Object](e TypeExtractor) Scheme[E] {
	return &scheme[E]{*NewTypeScheme[E](), e}
}

func (s *scheme[E]) Decode(data []byte) (E, error) {
	var _nil E

	ty, err := s.typeExtractor(data)
	if err != nil {
		return _nil, err
	}

	v, err := s.CreateObject(ty)
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
