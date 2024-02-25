package support

import (
	"sync"

	"github.com/mandelsoft/engine/pkg/processing/model/support/db"
	"github.com/mandelsoft/engine/pkg/utils"
)

type ExternalObjectSupport struct { // cannot use struct type here (Go)
	Lock sync.Mutex
	Wrapper
}

func (n *ExternalObjectSupport) GetDBObject() db.ExternalDBObject {
	return utils.Cast[db.ExternalDBObject](n.GetBase())
}
