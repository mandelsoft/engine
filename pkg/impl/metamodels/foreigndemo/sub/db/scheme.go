package db

import (
	"github.com/mandelsoft/engine/pkg/database"
	mymetamodel "github.com/mandelsoft/engine/pkg/metamodels/foreigndemo"
	"github.com/mandelsoft/engine/pkg/processing/model/support/db"
)

var Scheme = db.NewScheme[db.Object]()

func init() {
	database.MustRegisterType[db.Namespace, db.Object](Scheme)     // Goland requires second type parameter
	database.MustRegisterType[db.UpdateRequest, db.Object](Scheme) // Goland requires second type parameter
}

type Namespace = db.Namespace
type UpdateRequest = db.UpdateRequest

func NewUpdateRequest(ns, n string) *db.UpdateRequest {
	return &db.UpdateRequest{
		ObjectMeta: db.NewObjectMeta(mymetamodel.TYPE_UPDATEREQUEST, ns, n),
	}
}
