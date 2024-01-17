package runtime

import (
	"fmt"
)

type Encoding[T Object] interface {
	TypeNames() []string
	HasType(t string) bool
	CreateObject(typ string) (T, error)
	Decode(data []byte) (T, error)
}

type castingConverter[D, S Object] struct {
	encoding Encoding[S]
}

var _ Encoding[Object] = (*castingConverter[Object, Object])(nil)

func (c *castingConverter[D, S]) TypeNames() []string {
	return c.encoding.TypeNames()
}

func (c *castingConverter[D, S]) HasType(typ string) bool {
	return c.encoding.HasType(typ)
}

func (c *castingConverter[D, S]) CreateObject(typ string) (D, error) {
	var _nil D

	o, err := c.encoding.CreateObject(typ)
	if err != nil {
		return _nil, err
	}
	var i interface{} = o
	return i.(D), nil
}

func (c *castingConverter[D, S]) Decode(data []byte) (D, error) {
	var _nil D

	o, err := c.encoding.Decode(data)
	if err != nil {
		return _nil, err
	}
	var i interface{} = o
	return i.(D), nil
}

func ConvertEncoding[D, S Object](src Encoding[S]) (Encoding[D], error) {
	if !TypeOf[S]().AssignableTo(TypeOf[D]()) {
		return nil, fmt.Errorf("type %s is not assignable to %s", TypeOf[S](), TypeOf[D]())
	}
	return &castingConverter[D, S]{src}, nil
}
