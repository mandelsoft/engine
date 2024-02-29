package db

import (
	"github.com/mandelsoft/engine/pkg/database"
	"github.com/mandelsoft/engine/pkg/processing/model/support/db"
)

var Scheme = database.NewScheme[db.Object]()

func init() {
	database.MustRegisterType[db.Namespace, db.Object](Scheme) // Goland requires second type parameter
}
