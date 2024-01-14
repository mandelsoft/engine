package runtime

import (
	"fmt"
)

type Encoding[T Object] interface {
	Decode(data []byte) (T, error)
}

type castingConverter[D, S Object] struct {
	scheme Encoding[S]
}

var _ Encoding[Object] = (*castingConverter[Object, Object])(nil)

func (c castingConverter[D, S]) Decode(data []byte) (D, error) {
	var _nil D

	o, err := c.scheme.Decode(data)
	if err != nil {
		return _nil, err
	}
	var i interface{} = o
	return i.(D), nil
}

func EncoderView[D, S Object](src Scheme[S]) (Encoding[D], error) {
	if !TypeOf[S]().AssignableTo(TypeOf[D]()) {
		return nil, fmt.Errorf("type %s is not assignable to %s", TypeOf[S](), TypeOf[D]())
	}
	return &castingConverter[D, S]{src}, nil
}
