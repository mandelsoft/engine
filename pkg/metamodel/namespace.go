package metamodel

import (
	"github.com/mandelsoft/engine/pkg/database"
)

type Namespace interface {
	GenerationObject

	TryLock(db database.Database, id RunId) (bool, error)
}
