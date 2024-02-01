package db

import (
	"github.com/mandelsoft/engine/pkg/database"
	"github.com/mandelsoft/engine/pkg/metamodel/common"
	"github.com/mandelsoft/engine/pkg/metamodel/model"
	"github.com/mandelsoft/engine/pkg/metamodel/model/support"
	demo "github.com/mandelsoft/engine/pkg/metamodels/multidemo"
)

func init() {
	database.MustRegisterType[NodeState, support.DBObject](Scheme) // Goland requires second type parameter
}

type NodeState struct {
	support.InternalDBObjectSupport `json:",inline"`

	// shared state for all phases.
	// This stores the node state commonly fixed for all phases when the first phase is started.

	Current ObjectCurrentState `json:"current"`
	Target  *ObjectTargetState `json:"target,omitempty"`

	// phase specif states

	Gather struct {
		Current GatherCurrentState `json:"current"`
		Target  *GatherTargetState `json:"target,omitempty"`
	} `json: "gather"`
	Calculation struct {
		Current CalculationCurrentState `json:"current"`
		Target  *CalculationTargetState `json:"target,omitempty"`
	} `json: "calculation"`
}

var _ support.InternalDBObject = (*NodeState)(nil)

type ObjectCurrentState struct {
	Operands []string `json:"operands"`
}

type ObjectTargetState struct {
	Spec          NodeSpec `json:"spec"`
	ObjectVersion string   `json:"objectVersion"`
}

type GatherCurrentState struct {
	InputVersion  string       `json:"inputVersion"`
	ObjectVersion string       `json:"objectVersion"`
	OutputVersion string       `json:"outputVersion"`
	Output        GatherOutput `json:"output"`
}

type CalculationCurrentState struct {
	InputVersion  string            `json:"inputVersion"`
	ObjectVersion string            `json:"objectVersion"`
	OutputVersion string            `json:"outputVersion"`
	Output        CalculationOutput `json:"output"`
}

type GatherOutput struct {
	Values []Operand `json:"operands"`
}

type CalculationOutput struct {
	Value int `json:"value"`
}

type GatherTargetState struct {
	ObjectVersion string `json:"version"`
}

type CalculationTargetState struct {
	ObjectVersion string `json:"version"`
}

func (n *NodeState) CommitTargetState(lctx common.Logging, phase model.Phase, spec *model.CommitInfo) {
	log := lctx.Logger(REALM).WithValues("name", n.Name, "phase", phase)
	switch phase {
	case demo.PHASE_GATHER:
		if n.Gather.Target != nil && spec != nil {
			// update phase specific state
			log.Info("commit phase {{phase}} for NodeState {{name}}")
			log.Info("  input version {{inpvers}}", "inpvers", spec.InputVersion)
			log.Info("  object version {{objvers}}", "objvers", n.Gather.Target.ObjectVersion)
			log.Info("  output version {{outvers}}", "outvers", spec.State.(*GatherResultState).GetOutputVersion())
			log.Info("  output {{output}}", "output", spec.State.(*GatherResultState).GetState())
			c := &n.Gather.Current
			c.InputVersion = spec.InputVersion
			c.ObjectVersion = n.Gather.Target.ObjectVersion
			c.OutputVersion = spec.State.(*GatherResultState).GetOutputVersion()
			c.Output.Values = spec.State.(*GatherResultState).GetState()
		}
		n.Gather.Target = nil

	case demo.PHASE_CALCULATION:
		if n.Calculation.Target != nil && spec != nil {
			// update state specific
			log.Info("commit phase {{phase}} for NodeState {{name}}")
			log.Info("  input version {{inpvers}}", "inpvers", spec.InputVersion)
			log.Info("  object version {{objvers}}", "objvers", n.Calculation.Target.ObjectVersion)
			log.Info("  output version {{outvers}}", "outvers", spec.State.(*CalcResultState).GetOutputVersion())
			log.Info("  output {{output}}", "output", spec.State.(*CalcResultState).GetState())
			c := &n.Calculation.Current
			c.InputVersion = spec.InputVersion
			c.ObjectVersion = n.Calculation.Target.ObjectVersion
			c.OutputVersion = spec.State.(*CalcResultState).GetOutputVersion()
			c.Output.Value = spec.State.(*CalcResultState).GetState()

			// ... and common state for last phase
			log.Info("  operands {{operands}}", "operands", n.Target.Spec.Operands)
			n.Current.Operands = n.Target.Spec.Operands
		}
		n.Calculation.Target = nil
		n.Target = nil
	}
}

////////////////////////////////////////////////////////////////////////////////

type Operand struct {
	Origin model.ObjectId `json:"origin,omitempty"`
	Value  int            `json:"value"`
}

type GatherResultState = support.ResultState[[]Operand]
type CalcResultState = support.ResultState[int]

var NewGatherResultState = support.NewResultState[[]Operand]
var NewCalcResultState = support.NewResultState[int]
