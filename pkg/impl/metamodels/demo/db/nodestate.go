package db

import (
	"github.com/mandelsoft/engine/pkg/database"
	"github.com/mandelsoft/engine/pkg/metamodel/model/support"
)

func init() {
	database.MustRegisterType[NodeState, support.DBObject](Scheme) // Goland requires second type parameter
}

type NodeState struct {
	support.InternalDBObjectSupport `json:",inline"`
}
