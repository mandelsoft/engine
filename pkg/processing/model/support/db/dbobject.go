package db

import (
	"github.com/mandelsoft/engine/pkg/database"
)

type DBObject interface {
	database.Object
	database.GenerationAccess
	database.Finalizable
	database.StatusSource
}

type ObjectMeta struct {
	database.GenerationObjectMeta `json:",inline"`
	database.FinalizedMeta        `json:",inline"`
}

func NewObjectMeta(ty string, ns string, name string) ObjectMeta {
	return ObjectMeta{
		GenerationObjectMeta: database.NewGenerationObjectMeta(ty, ns, name),
	}
}
