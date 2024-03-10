package db

import (
	"github.com/mandelsoft/engine/pkg/database"
	"github.com/mandelsoft/engine/pkg/runtime"
)

func NewScheme[O Object]() database.Scheme[O] {
	return database.NewScheme[O](TypeExtractor)
}

var TypeExtractor = runtime.TypeExtractorFor[ObjectMeta]()
