package db

import (
	"github.com/mandelsoft/engine/pkg/database"
	"github.com/mandelsoft/engine/pkg/processing/model/support/db"
)

var Scheme = database.NewScheme[db.DBObject]()

func init() {
	database.MustRegisterType[db.Namespace, db.DBObject](Scheme) // Goland requires second type parameter
}
