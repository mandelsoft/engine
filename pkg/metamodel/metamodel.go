package metamodel

import (
	"github.com/mandelsoft/engine/pkg/database"
	"github.com/mandelsoft/engine/pkg/runtime"
)

type RunId string

type Object interface {
	database.Object

	Process() Status
}

type Status struct {
	Error error
}

type MetaModel interface {
	GetScheme() runtime.Scheme[Object]
	GetTypes() (external []string, internal []string)
}
