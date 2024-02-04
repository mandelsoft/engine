package hashmapped

import (
	"github.com/mandelsoft/engine/pkg/database"
	"github.com/mandelsoft/engine/pkg/processing/metamodel/objectbase"
	"github.com/mandelsoft/engine/pkg/runtime"
)

type Initializer = runtime.Initializer[objectbase.Object]

func SetObjectName(ns, n string) Initializer {
	return database.SetObjectName[objectbase.Object](ns, n)
}
