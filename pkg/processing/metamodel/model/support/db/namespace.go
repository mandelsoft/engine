package db

import (
	. "github.com/mandelsoft/engine/pkg/processing/mmids"

	"github.com/mandelsoft/engine/pkg/database"
	"github.com/mandelsoft/engine/pkg/processing/metamodel/model/support"
)

type Namespace struct {
	database.GenerationObjectMeta

	RunLock RunId `json:"runLock"`
}

var _ support.DBNamespace = (*Namespace)(nil)

func (n *Namespace) GetRunLock() RunId {
	return n.RunLock
}

func (n *Namespace) SetRunLock(id RunId) {
	n.RunLock = id
}
