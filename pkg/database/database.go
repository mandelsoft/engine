package database

import (
	"github.com/mandelsoft/engine/pkg/runtime"
)

type SchemeTypes[O Object] interface {
	runtime.SchemeTypes[O]
}

type Database[O Object] interface {
	SchemeTypes() SchemeTypes[O]

	HandlerRegistration
	ObjectLister
	ListObjects(typ string, ns string) ([]O, error)

	GetObject(ObjectId) (O, error)
	SetObject(O) error
	DeleteObject(id ObjectId) error
}
