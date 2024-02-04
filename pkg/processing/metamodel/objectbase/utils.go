package objectbase

import (
	"github.com/mandelsoft/engine/pkg/database"
)

func SetObjectName(ns, n string) Initializer {
	return database.SetObjectName[Object](ns, n)
}
