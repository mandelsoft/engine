package demo

import (
	"github.com/mandelsoft/engine/pkg/database"
	"github.com/mandelsoft/engine/pkg/metamodel"
)

type NodeState struct {
	database.ObjectMeta
}

func (n *NodeState) Process() metamodel.Status {
	return metamodel.Status{}
}
