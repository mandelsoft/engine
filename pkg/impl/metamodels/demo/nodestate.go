package demo

import (
	"github.com/mandelsoft/engine/pkg/metamodel/model"
	_default "github.com/mandelsoft/engine/pkg/metamodel/model/default"
)

func init() {
	model.MustRegisterType[NodeState](scheme)
}

type NodeState struct {
	_default.InternalObject `json:",inline"`
}

var _ model.InternalObject = (*NodeState)(nil)

func (n *NodeState) GetState(phase model.Phase) model.State {
	return nil
}

func (n *NodeState) GetTargetState(phase model.Phase) model.State {
	return nil
}

func (n *NodeState) Process(req model.Request) model.Status {
	return model.Status{}
}
