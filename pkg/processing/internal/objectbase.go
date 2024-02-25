package internal

import (
	"github.com/mandelsoft/engine/pkg/database"
)

type Objectbase interface {
	database.Database[Object]
	CreateObject(database.ObjectId) (Object, error)
}

type Object interface {
	database.Object

	database.GenerationAccess

	GetFinalizers() []string
	AddFinalizer(ob Objectbase, f string) (bool, error)
	RemoveFinalizer(ob Objectbase, f string) (bool, error)
	HasFinalizer(f string) bool

	IsDeleting() bool
}
