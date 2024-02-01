package db

import (
	"github.com/mandelsoft/engine/pkg/database"
	"github.com/mandelsoft/engine/pkg/metamodel/common"
	"github.com/mandelsoft/engine/pkg/metamodel/model"
	"github.com/mandelsoft/engine/pkg/metamodel/model/support"
)

func init() {
	database.MustRegisterType[ValueState, support.DBObject](Scheme) // Goland requires second type parameter
}

type ValueState struct {
	support.InternalDBObjectSupport `json:",inline"`

	Current CurrentState `json:"current"`
	Target  *TargetState `json:"target,omitempty"`
}

var _ support.InternalDBObject = (*ValueState)(nil)

type CurrentState struct {
	Owner         string `json:"ownwer,omitempty"`
	InputVersion  string `json:"inputVersion"`
	ObjectVersion string `json:"objectVersion"`
	OutputVersion string `json:"outputVersion"`
	Output        Output `json:"output"`
}

type Output struct {
	Value int `json:"value"`
}

type TargetState struct {
	ObjectVersion string    `json:"version"`
	Spec          ValueSpec `json:"spec"`
}

func (n *ValueState) CommitTargetState(lctx common.Logging, phase model.Phase, spec *model.CommitInfo) {
	log := lctx.Logger(REALM)
	if n.Target != nil && spec != nil {
		n.Current.InputVersion = spec.InputVersion
		log.Info("Commit object version for ValueState {{name}}", "name", n.Name)
		log.Info("  object version {{version}}", "version", n.Target.ObjectVersion)
		n.Current.ObjectVersion = n.Target.ObjectVersion
		n.Current.OutputVersion = spec.State.(*ValueResultState).GetOutputVersion()
		n.Current.Output.Value = spec.State.(*ValueResultState).GetState().Value

		log.Info("  object version {{owner}}", "owner", n.Target.Spec.Owner)
		n.Current.Owner = n.Target.Spec.Owner
	}
	n.Target = nil
}

////////////////////////////////////////////////////////////////////////////////

type ValueResult struct {
	Origin model.ObjectId `json:"origin,omitempty"`
	Value  int            `json:"value"`
}

type ValueResultState = support.ResultState[ValueResult]

var NewValueResultState = support.NewResultState[ValueResult]
