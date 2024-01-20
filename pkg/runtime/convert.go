package runtime

import (
	"fmt"

	"github.com/mandelsoft/engine/pkg/utils"
)

////////////////////////////////////////////////////////////////////////////////

func converFunc[D, S Object](f Initializer[D]) Initializer[S] {
	return func(o S) {
		f(utils.Cast[D](o))
	}
}

func convertFuncs[D, S Object](fs ...Initializer[D]) []Initializer[S] {
	var r []Initializer[S]
	for _, f := range fs {
		r = append(r, converFunc[D, S](f))
	}
	return r
}

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

func (c *castingTypes[D, S]) CreateObject(typ string, init ...Initializer[D]) (D, error) {
	var _nil D

	o, err := c.types.CreateObject(typ, convertFuncs[D, S](init...)...)
	if err != nil {
		return _nil, err
	}
	return utils.Cast[D](o), nil
}

func ConvertTypes[D, S Object](src SchemeTypes[S]) (SchemeTypes[D], error) {
	if !utils.TypeOf[S]().AssignableTo(utils.TypeOf[D]()) {
		return nil, fmt.Errorf("type %s is not assignable to %s", utils.TypeOf[S](), utils.TypeOf[D]())
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

func (c *castingConverter[D, S]) CreateObject(typ string, init ...Initializer[D]) (D, error) {
	var _nil D

	o, err := c.encoding.CreateObject(typ, convertFuncs[D, S](init...)...)
	if err != nil {
		return _nil, err
	}
	return utils.Cast[D](o), nil
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
	if !utils.TypeOf[S]().AssignableTo(utils.TypeOf[D]()) {
		return nil, fmt.Errorf("type %s is not assignable to %s", utils.TypeOf[S](), utils.TypeOf[D]())
	}
	return &castingConverter[D, S]{src}, nil
}
