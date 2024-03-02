package explicit

import (
	"fmt"

	. "github.com/mandelsoft/engine/pkg/processing/mmids"

	"github.com/mandelsoft/logging"

	"github.com/mandelsoft/engine/pkg/processing/model"
	"github.com/mandelsoft/engine/pkg/processing/model/support"

	"github.com/mandelsoft/engine/pkg/impl/metamodels/valopdemo/explicit/db"
	mymetamodel "github.com/mandelsoft/engine/pkg/metamodels/valopdemo"
)

type CalculatePhase struct{ PhaseBase }

var _ OperatorStatePhase = (*CalculatePhase)(nil)

func (_ CalculatePhase) GetCurrentState(o *OperatorState, phase Phase) model.CurrentState {
	return NewCurrentCalcState(o)
}

func (_ CalculatePhase) GetTargetState(o *OperatorState, phase Phase) model.TargetState {
	return NewTargetCalcState(o)
}

func (_ CalculatePhase) DBSetExternalState(log logging.Logger, o *db.OperatorState, phase Phase, state model.ExternalState, mod *bool) {
	log.Info("set target state for phase {{phase}} of Operator {{name}}")
	support.UpdateField(&o.Calculation.Target.ObjectVersion, &o.Gather.Current.ObjectVersion, mod)
}

func (_ CalculatePhase) DBRollback(log logging.Logger, o *db.OperatorState, phase Phase, mod *bool) {
}

func (_ CalculatePhase) DBCommit(log logging.Logger, o *db.OperatorState, phase Phase, spec *model.CommitInfo, mod *bool) {
	if spec != nil {
		log.Info("  output {{output}}", "output", spec.OutputState.(*CalcOutputState).GetState())
		c := &o.Calculation.Current
		c.Output.Value = spec.OutputState.(*CalcOutputState).GetState()
	} else {
		log.Info("nothing to commit for phase {{phase}} of OperatorState {{name}}")
	}
}

func (_ CalculatePhase) Process(o *OperatorState, phase Phase, req model.Request) model.ProcessingResult {
	log := req.Logging.Logger(REALM)

	var inp *db.GatherOutput
	for _, l := range req.Inputs {
		inp = l.(*GatherOutputState).GetState()
	}
	op := inp.Operator

	out := inp.Operands[0].Value
	log.Info("calculate {{operator}} {{operands}}", "operator", op, "operands", inp.Operands)
	switch op {
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

	return model.StatusCompleted(NewCalcOutputState(req.FormalVersion, out))
}

////////////////////////////////////////////////////////////////////////////////

type CalcOutputState = support.OutputState[int]

var NewCalcOutputState = support.NewOutputState[int]

////////////////////////////////////////////////////////////////////////////////

type CurrentCalcState struct {
	support.CurrentStateSupport[*db.OperatorState, *db.CalculationCurrentState]
}

func NewCurrentCalcState(n *OperatorState) model.CurrentState {
	return &CurrentCalcState{support.NewCurrentStateSupport[*db.OperatorState, *db.CalculationCurrentState](n, mymetamodel.PHASE_CALCULATION)}
}

func (c *CurrentCalcState) GetObservedState() model.ObservedState {
	return c.GetObservedStateForPhase(mymetamodel.PHASE_GATHER)
}

func (c *CurrentCalcState) GetLinks() []ElementId {
	return []ElementId{c.PhaseLink(mymetamodel.PHASE_GATHER)}
}

func (c *CurrentCalcState) GetOutput() model.OutputState {
	return NewCalcOutputState(c.GetFormalVersion(), c.Get().Output.Value)
}

////////////////////////////////////////////////////////////////////////////////

type TargetCalcState struct {
	support.TargetStateSupport[*db.OperatorState, *db.CalculationTargetState]
}

var _ model.TargetState = (*TargetCalcState)(nil)

func NewTargetCalcState(n *OperatorState) *TargetCalcState {
	return &TargetCalcState{support.NewTargetStateSupport[*db.OperatorState, *db.CalculationTargetState](n, mymetamodel.PHASE_CALCULATION)}
}

func (c *TargetCalcState) GetLinks() []ElementId {
	return []ElementId{c.PhaseLink(mymetamodel.PHASE_GATHER)}
}
