package db

import (
	"github.com/mandelsoft/engine/pkg/database"
	"github.com/mandelsoft/engine/pkg/processing/metamodel/model/support"
	"github.com/mandelsoft/engine/pkg/processing/metamodel/model/support/db"
)

var Scheme = database.NewScheme[support.DBObject]()

func init() {
	database.MustRegisterType[db.Namespace, support.DBObject](Scheme) // Goland requires second type parameter
}
