package demo

import (
	"github.com/mandelsoft/engine/pkg/database"
	"github.com/mandelsoft/engine/pkg/metamodel/model"
	"github.com/mandelsoft/engine/pkg/metamodel/model/common"
)

func init() {
	model.MustRegisterType[Node](scheme)
}

type Node struct {
	database.GenerationObjectMeta
}

var _ model.ExternalObject = (*Node)(nil)

func (n *Node) GetState() model.State {
	return nil
}

func (n *Node) Process(req model.Request) common.Status {
	return model.Status{}
}
