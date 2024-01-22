package demo

import (
	"github.com/mandelsoft/engine/pkg/metamodel/model"
	"github.com/mandelsoft/engine/pkg/metamodel/model/objectbase/wrapped"
	"github.com/mandelsoft/engine/pkg/metamodel/model/support"
)

func init() {
	wrapped.MustRegisterType[Node](scheme)
}

type Node struct {
	support.ExternalObjectSupport
}

var _ model.ExternalObject = (*Node)(nil)

func (n *Node) GetState() model.State {
	return nil
}
