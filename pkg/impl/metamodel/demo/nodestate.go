package demo

import (
	"github.com/mandelsoft/engine/pkg/database"
	"github.com/mandelsoft/engine/pkg/metamodel"
)

func init() {
	metamodel.MustRegisterType[NodeState](scheme)
}

type NodeState struct {
	database.GenerationObjectMeta
}

var _ metamodel.Object = (*NodeState)(nil)

func (n *NodeState) Process(req metamodel.Request) metamodel.Status {
	return metamodel.Status{}
}
