package support

import (
	"github.com/mandelsoft/engine/pkg/database"
	"github.com/mandelsoft/engine/pkg/wrapper"
)

type DBObject interface {
	database.Object
	database.GenerationAccess
}

type _DBObject = DBObject

type Wrapper[O DBObject] struct {
	_DBObject
}

var _ wrapper.Object[DBObject] = (*Wrapper[DBObject])(nil)

func NewDBWrapper[O DBObject](o O) Wrapper[O] {
	return Wrapper[O]{o}
}

func (w *Wrapper[O]) SetBase(s O) {
	w._DBObject = s
}

func (w *Wrapper[O]) GetBase() O {
	return w._DBObject.(O)
}

////////////////////////////////////////////////////////////////////////////////

type IdentityMapping[O database.Object] struct{}

var _ wrapper.IdMapping[database.Object] = (*IdentityMapping[database.Object])(nil)

func (i IdentityMapping[O]) Namespace(s string) string {
	return s
}

func (i IdentityMapping[O]) Inbound(id wrapper.ObjectId) wrapper.ObjectId {
	return id
}

func (i IdentityMapping[O]) Outbound(id wrapper.ObjectId) wrapper.ObjectId {
	return id
}

func (i IdentityMapping[O]) OutboundObject(o O) wrapper.ObjectId {
	return database.NewObjectIdFor(o)
}
