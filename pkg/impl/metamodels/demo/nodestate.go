package demo

import (
	"github.com/mandelsoft/engine/pkg/metamodel/model"
	"github.com/mandelsoft/engine/pkg/metamodel/model/support"
	"github.com/mandelsoft/engine/pkg/metamodel/objectbase"
	"github.com/mandelsoft/engine/pkg/metamodel/objectbase/wrapped"
)

func init() {
	wrapped.MustRegisterType[NodeState](scheme)
}

type NodeState struct {
	support.InternalObjectSupport
}

var _ model.InternalObject = (*NodeState)(nil)

func (n *NodeState) GetState(phase model.Phase) model.State {
	return nil
}

func (n *NodeState) GetTargetState(phase model.Phase) model.State {
	return nil
}

func (n *NodeState) Process(ob objectbase.Objectbase, req model.Request) model.Status {
	return model.Status{}
}
