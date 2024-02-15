package db

import (
	"github.com/mandelsoft/engine/pkg/processing/mmids"
)

type DBNamespace interface {
	DBObject
	GetRunLock() mmids.RunId
	SetRunLock(id mmids.RunId)
}
