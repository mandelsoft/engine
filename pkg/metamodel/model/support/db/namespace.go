package db

import (
	"github.com/mandelsoft/engine/pkg/database"
	"github.com/mandelsoft/engine/pkg/metamodel/common"
	"github.com/mandelsoft/engine/pkg/metamodel/model/support"
)

type Namespace struct {
	database.GenerationObjectMeta

	RunLock common.RunId `json:"runLock"`
}

var _ support.DBNamespace = (*Namespace)(nil)

func (n *Namespace) GetRunLock() common.RunId {
	return n.RunLock
}

func (n *Namespace) SetRunLock(id common.RunId) {
	n.RunLock = id
}
