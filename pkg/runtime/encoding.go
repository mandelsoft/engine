package runtime

import (
	"fmt"
)

type SchemeTypes[T Object] interface {
	TypeNames() []string
	HasType(t string) bool
	CreateObject(typ string) (T, error)
}

type Encoding[T Object] interface {
	SchemeTypes[T]
	Decode(data []byte) (T, error)
}

////////////////////////////////////////////////////////////////////////////////

type castingTypes[D, S Object] struct {
	types SchemeTypes[S]
}

var _ SchemeTypes[Object] = (*castingTypes[Object, Object])(nil)

func (c *castingTypes[D, S]) TypeNames() []string {
	return c.types.TypeNames()
}

func (c *castingTypes[D, S]) HasType(typ string) bool {
	return c.types.HasType(typ)
}

func (c *castingTypes[D, S]) CreateObject(typ string) (D, error) {
	var _nil D

	o, err := c.types.CreateObject(typ)
	if err != nil {
		return _nil, err
	}
	var i interface{} = o
	return i.(D), nil
}

func ConvertTypes[D, S Object](src SchemeTypes[S]) (SchemeTypes[D], error) {
	if !TypeOf[S]().AssignableTo(TypeOf[D]()) {
		return nil, fmt.Errorf("type %s is not assignable to %s", TypeOf[S](), TypeOf[D]())
	}
	return &castingTypes[D, S]{src}, nil
}

////////////////////////////////////////////////////////////////////////////////

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
