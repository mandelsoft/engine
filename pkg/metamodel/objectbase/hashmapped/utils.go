package hashmapped

import (
	"github.com/mandelsoft/engine/pkg/database"
	"github.com/mandelsoft/engine/pkg/metamodel/common"
	"github.com/mandelsoft/engine/pkg/runtime"
)

type Initializer = runtime.Initializer[common.Object]

func SetObjectName(ns, n string) Initializer {
	return database.SetObjectName[common.Object](ns, n)
}
