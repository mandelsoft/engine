package db

import (
	"github.com/mandelsoft/engine/pkg/database"
)

type ExternalDBObject interface {
	Object
	database.StatusSource
}
