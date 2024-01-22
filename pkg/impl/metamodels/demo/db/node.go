package db

import (
	"github.com/mandelsoft/engine/pkg/database"
	"github.com/mandelsoft/engine/pkg/metamodel/model/support"
)

func init() {
	database.MustRegisterType[Node, support.DBObject](Scheme) // Goland requires second type parameter
}

type Node struct {
	database.GenerationObjectMeta
}

var _ database.Object = (*Node)(nil)
