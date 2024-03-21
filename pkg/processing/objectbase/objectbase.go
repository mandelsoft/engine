package objectbase

import (
	"github.com/mandelsoft/engine/pkg/database"
	"github.com/mandelsoft/engine/pkg/database/wrapper"
	"github.com/mandelsoft/engine/pkg/runtime"
)

type EventHandler = database.EventHandler
type Initializer = runtime.Initializer[Object]

func NewScheme() Scheme {
	return database.NewScheme[Object](nil)
}

type pointer[P any] interface {
	Object
	*P
}

func MustRegisterType[T any, P pointer[T]](s database.TypeScheme[Object]) { // Goland: should be Scheme
	database.MustRegisterType[T, Object, P](s)
}

type objectbase struct {
	database.Database[Object]
}

func NewObjectbase(db database.Database[Object]) Objectbase {
	return &objectbase{db}
}

func (d *objectbase) CreateObject(id database.ObjectId) (Object, error) {
	return d.SchemeTypes().CreateObject(id.GetType(), SetObjectName(id.GetNamespace(), id.GetName()))
}

func (d *objectbase) GetDatabase() database.Database[Object] {
	return d.Database
}

func GetDatabase[O database.Object](ob Objectbase) database.Database[O] {
	if w, ok := ob.(wrapper.Wrapped[O]); ok {
		return w.GetDatabase()
	}
	if w, ok := ob.(wrapper.Wrapped[Object]); ok {
		db := w.GetDatabase()
		if w, ok := db.(wrapper.Wrapped[O]); ok {
			return w.GetDatabase()
		}
	}
	return nil
}

////////////////////////////////////////////////////////////////////////////////

type lister struct {
	ob Objectbase
}

var _ database.ObjectLister = (*lister)(nil)

func (l lister) ListObjectIds(typ string, closure bool, ns string, atomic ...func()) ([]database.ObjectId, error) {
	return l.ob.ListObjectIds(typ, closure, ns, atomic...)
}
