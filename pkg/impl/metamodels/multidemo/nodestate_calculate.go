package multidemo

import (
	"fmt"

	. "github.com/mandelsoft/engine/pkg/processing/mmids"

	"github.com/mandelsoft/logging"

	"github.com/mandelsoft/engine/pkg/processing/model"
	"github.com/mandelsoft/engine/pkg/processing/model/support"

	"github.com/mandelsoft/engine/pkg/impl/metamodels/multidemo/db"
	mymetamodel "github.com/mandelsoft/engine/pkg/metamodels/multidemo"
)

type CalculatePhase struct{ PhaseBase }

var _ NodeStatePhase = (*CalculatePhase)(nil)

func (_ CalculatePhase) GetCurrentState(o *NodeState, phase Phase) model.CurrentState {
	return NewCurrentCalcState(o)
}

func (c CalculatePhase) GetTargetState(o *NodeState, phase Phase) model.TargetState {
	return c.getTargetState(o)
}

func (_ CalculatePhase) getTargetState(o *NodeState) *TargetCalcState {
	return NewTargetCalcState(o)
}

func (c CalculatePhase) DBSetExternalState(log logging.Logger, o *db.NodeState, phase Phase, state model.ExternalState, mod *bool) {
	// no external state for this phase -> use object version from gather phase
	support.UpdateField(&o.Calculation.Target.ObjectVersion, &o.Gather.Current.ObjectVersion, mod)
}

func (_ CalculatePhase) DBCommit(log logging.Logger, o *db.NodeState, phase Phase, spec *model.CommitInfo, mod *bool) {
	if spec != nil {
		// update state specific
		log.Info("  output {{output}}", "output", spec.OutputState.(*CalcOutputState).GetState())
		c := &o.Calculation.Current
		c.Output.Value = spec.OutputState.(*CalcOutputState).GetState()
	}
}

func (_ CalculatePhase) Process(o *NodeState, phase Phase, req model.Request) model.ProcessingResult {
	log := req.Logging.Logger(REALM)

	var inp *db.GatherOutput

	for _, l := range req.Inputs {
		inp = l.(*GatherOutputState).GetState()
	}
	op := inp.Operator

	out := inp.Operands[0].Value
	if op != nil {
		log.Info("calculate {{operator}} {{operands}}", "operator", *op, "operands", inp.Operands)
		switch *op {
		case db.OP_ADD:
			for _, v := range inp.Operands[1:] {
				out += v.Value
			}
		case db.OP_SUB:
			for _, v := range inp.Operands[1:] {
				out -= v.Value
			}
		case db.OP_MUL:
			for _, v := range inp.Operands[1:] {
				out *= v.Value
			}
		case db.OP_DIV:
			for i, v := range inp.Operands[1:] {
				if v.Value == 0 {
					return model.StatusFailed(fmt.Errorf("division by zero for operand %d[%s]", i, inp.Operands[i+1].Origin))
				}
				out /= v.Value
			}
		}
	} else {
		log.Info("use input value {{input}}}", "input", out)
	}

	return model.StatusCompleted(NewCalcOutputState(req.FormalVersion, out))
}

///////////////////////////////////////////////////////////////////////////////

type CalcOutputState = support.OutputState[int]

var NewCalcOutputState = support.NewOutputState[int]

////////////////////////////////////////////////////////////////////////////////

type CurrentCalcState struct {
	support.CurrentStateSupport[*db.NodeState, *db.CalculationCurrentState]
}

func NewCurrentCalcState(n *NodeState) model.CurrentState {
	return &CurrentCalcState{support.NewCurrentStateSupport[*db.NodeState, *db.CalculationCurrentState](n, mymetamodel.PHASE_CALCULATION)}
}

func (c *CurrentCalcState) GetLinks() []ElementId {
	return []ElementId{c.PhaseLink(mymetamodel.PHASE_GATHER)}
}

func (c *CurrentCalcState) GetOutput() model.OutputState {
	return NewCalcOutputState(c.GetFormalVersion(), c.Get().Output.Value)
}

////////////////////////////////////////////////////////////////////////////////

type TargetGatherState struct {
	support.TargetStateSupport[*db.NodeState, *db.GatherTargetState]
}

var _ model.TargetState = (*TargetGatherState)(nil)

func NewTargetGatherState(n *NodeState) *TargetGatherState {
	return &TargetGatherState{support.NewTargetStateSupport[*db.NodeState, *db.GatherTargetState](n, mymetamodel.PHASE_GATHER)}
}

func (c *TargetGatherState) GetLinks() []ElementId {
	var r []ElementId

	t := c.Get()
	if t == nil {
		return nil
	}

	for _, o := range t.Spec.Operands {
		r = append(r, NewElementId(c.GetType(), c.GetNamespace(), o, mymetamodel.PHASE_CALCULATION))
	}
	return r
}

func (c *TargetGatherState) GetOperator() *db.OperatorName {
	return c.Get().Spec.Operator
}

func (c *TargetGatherState) GetValue() *int {
	return c.Get().Spec.Value
}

////////////////////////////////////////////////////////////////////////////////

type TargetCalcState struct {
	support.TargetStateSupport[*db.NodeState, *db.CalculationTargetState]
}

var _ model.TargetState = (*TargetCalcState)(nil)

func NewTargetCalcState(n *NodeState) *TargetCalcState {
	return &TargetCalcState{support.NewTargetStateSupport[*db.NodeState, *db.CalculationTargetState](n, mymetamodel.PHASE_CALCULATION)}
}

func (c *TargetCalcState) GetLinks() []ElementId {
	return []ElementId{c.PhaseLink(mymetamodel.PHASE_GATHER)}
}
