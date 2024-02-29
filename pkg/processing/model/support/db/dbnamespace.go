package db

import (
	"github.com/mandelsoft/engine/pkg/processing/mmids"
)

type DBNamespace interface {
	Object
	GetRunLock() mmids.RunId
	SetRunLock(id mmids.RunId)
}
