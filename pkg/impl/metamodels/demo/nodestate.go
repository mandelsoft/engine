package demo

import (
	"github.com/mandelsoft/engine/pkg/database"
	"github.com/mandelsoft/engine/pkg/metamodel/model/common"
)

func init() {
	common.MustRegisterType[NodeState](scheme)
}

type NodeState struct {
	database.GenerationObjectMeta
}

var _ common.Object = (*NodeState)(nil)

func (n *NodeState) Process(req common.Request) common.Status {
	return common.Status{}
}
