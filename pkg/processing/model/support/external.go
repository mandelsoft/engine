package support

import (
	"sync"

	"github.com/mandelsoft/engine/pkg/database"
	"github.com/mandelsoft/engine/pkg/processing/model/support/db"
	"github.com/mandelsoft/engine/pkg/utils"
)

type ExternalDBObject interface {
	db.DBObject
	database.StatusSource
}

type ExternalObjectSupport struct { // cannot use struct type here (Go)
	Lock sync.Mutex
	Wrapper
}

func (n *ExternalObjectSupport) GetDBObject() ExternalDBObject {
	return utils.Cast[ExternalDBObject](n.GetBase())
}
