package common

import (
	"github.com/mandelsoft/engine/pkg/database"
)

type Namespace interface {
	Object

	TryLock(db database.Database, id RunId) (bool, error)
}
