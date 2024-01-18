package demo

import (
	"github.com/mandelsoft/engine/pkg/database"
	"github.com/mandelsoft/engine/pkg/metamodel/model"
	"github.com/mandelsoft/engine/pkg/metamodel/model/common"
)

func init() {
	common.MustRegisterType[Node](scheme)
}

type Node struct {
	database.GenerationObjectMeta
}

var _ model.ExternalObject = (*Node)(nil)

func (n *Node) Process(req common.Request) common.Status {
	return common.Status{}
}
