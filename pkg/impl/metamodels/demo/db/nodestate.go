package db

import (
	"github.com/mandelsoft/engine/pkg/database"
	"github.com/mandelsoft/engine/pkg/metamodel/common"
	"github.com/mandelsoft/engine/pkg/metamodel/model"
	"github.com/mandelsoft/engine/pkg/metamodel/model/support"
)

func init() {
	database.MustRegisterType[NodeState, support.DBObject](Scheme) // Goland requires second type parameter
}

type NodeState struct {
	support.InternalDBObjectSupport `json:",inline"`

	Current CurrentState `json:"current"`
	Target  *TargetState `json:"target,omitempty"`
}

var _ support.InternalDBObject = (*NodeState)(nil)

type CurrentState struct {
	Operands      []string `json:"operands"`
	InputVersion  string   `json:"inputVersion"`
	ObjectVersion string   `json:"objectVersion"`
	OutputVersion string   `json:"outputVersion"`
	Output        Output   `json:"output"`
}

type Output struct {
	Value int `json:"value"`
}

type TargetState struct {
	ObjectVersion string   `json:"version"`
	Spec          NodeSpec `json:"spec"`
}

func (n *NodeState) CommitTargetState(lctx common.Logging, phase model.Phase, spec *model.CommitInfo) {
	log := lctx.Logger(REALM)
	if n.Target != nil && spec != nil {
		n.Current.Operands = n.Target.Spec.Operands
		n.Current.InputVersion = spec.InputVersion
		log.Info("Commit object version for NodeState {{name}}", "name", n.Name)
		log.Info("  object version {{version}}", "version", n.Target.ObjectVersion)
		n.Current.ObjectVersion = n.Target.ObjectVersion
		n.Current.OutputVersion = spec.State.(*ResultState).GetOutputVersion()
		n.Current.Output.Value = spec.State.(*ResultState).GetState()
	}
	n.Target = nil
}

////////////////////////////////////////////////////////////////////////////////

type ResultState = support.ResultState[int]

var NewResultState = support.NewResultState[int]
