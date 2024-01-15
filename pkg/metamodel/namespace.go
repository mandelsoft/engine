package metamodel

import (
	"github.com/mandelsoft/engine/pkg/database"
)

type Namespace struct {
	database.ObjectMeta

	RunLock RunId `json:"runLock"`
}

var _ Object = (*Namespace)(nil)

func (n *Namespace) Process(req Request) Status {
	return Status{}
}
