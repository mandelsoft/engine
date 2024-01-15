package demo

import (
	"github.com/mandelsoft/engine/pkg/database"
	"github.com/mandelsoft/engine/pkg/metamodel"
)

func init() {
	metamodel.MustRegisterType[Node](scheme)
}

type Node struct {
	database.ObjectMeta
}

var _ metamodel.Object = (*Node)(nil)

func (n *Node) Process(req metamodel.Request) metamodel.Status {
	return metamodel.Status{}
}
