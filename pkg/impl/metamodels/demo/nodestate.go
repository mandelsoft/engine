package demo

import (
	"github.com/mandelsoft/engine/pkg/metamodel/model"
	"github.com/mandelsoft/engine/pkg/metamodel/model/common"
	_default "github.com/mandelsoft/engine/pkg/metamodel/model/default"
)

func init() {
	common.MustRegisterType[NodeState](scheme)
}

type NodeState struct {
	_default.InternalObject `json:",inline"`
}

var _ model.InternalObject = (*NodeState)(nil)

func (n *NodeState) GetState() common.State {
	return nil
}

func (n *NodeState) GetTargetState() common.State {
	return nil
}

func (n *NodeState) Process(req common.Request) common.Status {
	return common.Status{}
}
