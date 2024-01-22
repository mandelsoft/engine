package support

import (
	"sync"

	"github.com/mandelsoft/engine/pkg/utils"
)

type ExternalDBObject interface {
	DBObject
}

type ExternalObjectSupport struct { // cannot use struct type here (Go)
	Lock sync.Mutex
	Wrapper
}

func (n *ExternalObjectSupport) GetDBObject() ExternalDBObject {
	return utils.Cast[ExternalDBObject](n.GetBase())
}
