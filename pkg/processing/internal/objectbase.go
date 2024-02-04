package internal

import (
	"github.com/mandelsoft/engine/pkg/database"
)

type Objectbase interface {
	database.Database[Object]
	CreateObject(database.ObjectId) (Object, error)
}
