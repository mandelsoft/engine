package demo

import (
	"github.com/mandelsoft/engine/pkg/database"
	"github.com/mandelsoft/engine/pkg/metamodel"
)

type Node struct {
	database.ObjectMeta
}

func (n *Node) Process() metamodel.Status {
	return metamodel.Status{}
}
