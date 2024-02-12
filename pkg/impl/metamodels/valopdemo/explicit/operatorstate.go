package explicit

import (
	"fmt"

	. "github.com/mandelsoft/engine/pkg/processing/mmids"

	"github.com/mandelsoft/engine/pkg/processing/metamodel/objectbase/wrapped"
	"github.com/mandelsoft/engine/pkg/processing/mmids"
	"github.com/mandelsoft/engine/pkg/processing/model"
	"github.com/mandelsoft/engine/pkg/processing/model/support"
	"github.com/mandelsoft/engine/pkg/runtime"
	"github.com/mandelsoft/logging"

	"github.com/mandelsoft/engine/pkg/impl/metamodels/valopdemo/explicit/db"
	mymetamodel "github.com/mandelsoft/engine/pkg/metamodels/valopdemo"
)

func init() {
	wrapped.MustRegisterType[OperatorState](scheme)
}

type OperatorState struct {
	support.InternalPhaseObjectSupport[*OperatorState, *db.OperatorState, *ExternalOperatorState] `json:",inline"`
}

var _ runtime.InitializedObject = (*OperatorState)(nil)

func (n *OperatorState) Initialize() error {
	return support.SetSelf(n, nodeStatePhases, db.OperatorPhaseStateAccess)
}

var _ model.InternalObject = (*OperatorState)(nil)

var nodeStatePhases = support.NewPhases[*OperatorState, *db.OperatorState, *ExternalOperatorState](REALM)

func init() {
	nodeStatePhases.Register(mymetamodel.PHASE_GATHER, GatherPhase{})
	nodeStatePhases.Register(mymetamodel.PHASE_CALCULATION, CalculatePhase{})
}

type OperatorStatePhase = support.Phase[*OperatorState, *db.OperatorState, *ExternalOperatorState]

////////////////////////////////////////////////////////////////////////////////

type PhaseBase struct {
	support.DefaultPhase[*OperatorState, *db.OperatorState]
}

func (_ PhaseBase) AcceptExternalState(lctx model.Logging, o *OperatorState, state model.ExternalStates, phase mmids.Phase) (model.AcceptStatus, error) {
	for _, _s := range state {
		s := _s.(*ExternalOperatorState).GetState()

		op := s.Operator
		if op == "" {
			return model.ACCEPT_INVALID, fmt.Errorf("operator missing")
		}

		if len(s.Operands) == 0 {
			return model.ACCEPT_INVALID, fmt.Errorf("operator node requires at least one operand")
		}
		switch op {
		case db.OP_ADD:
		case db.OP_SUB:
		case db.OP_DIV:
		case db.OP_MUL:
		default:
			return model.ACCEPT_INVALID, fmt.Errorf("unknown operator %q", op)
		}
	}
	return model.ACCEPT_OK, nil
}

////////////////////////////////////////////////////////////////////////////////
// Gather Phase
////////////////////////////////////////////////////////////////////////////////

type GatherPhase struct{ PhaseBase }

var _ OperatorStatePhase = (*GatherPhase)(nil)

func (g GatherPhase) GetCurrentState(o *OperatorState, phase Phase) model.CurrentState {
	return NewCurrentGatherState(o)
}

func (g GatherPhase) GetTargetState(o *OperatorState, phase Phase) model.TargetState {
	return NewTargetGatherState(o)
}

func (g GatherPhase) DBSetExternalState(log logging.Logger, o *db.OperatorState, phase Phase, state *ExternalOperatorState, mod *bool) {
	t := o.Gather.Target
	log.Info("set target state for phase {{phase}} of OperatorState {{name}}")
	support.UpdateField(&t.Spec, state.GetState(), mod)
}

func (g GatherPhase) DBCommit(log logging.Logger, o *db.OperatorState, phase Phase, spec *model.CommitInfo, mod *bool) {
	if spec != nil {
		c := &o.Gather.Current
		log.Info("  operands {{operands}}", "operands", o.Gather.Target.Spec.Operands)
		c.Operands = o.Gather.Target.Spec.Operands
		log.Info("  output {{output}}", "output", spec.OutputState.(*GatherOutputState).GetState())
		c.Output.Values = spec.OutputState.(*GatherOutputState).GetState()
	}
}

func (g GatherPhase) Process(o *OperatorState, phase Phase, req model.Request) model.ProcessingResult {
	log := req.Logging.Logger()

	links := NewTargetGatherState(o).GetLinks()
	operands := make([]db.Operand, len(links))
	for iid, e := range req.Inputs {
		s := e.(*ValueOutputState).GetState()
		for i, oid := range links {
			if iid == oid {
				operands[i] = db.Operand{
					Origin: iid.ObjectId(),
					Value:  s.Value,
				}
				log.Info("found operand {{index}} from {{link}}: {{value}}", "index", i, "link", iid, "value", operands[i].Value)
				break
			}
		}
	}

	if len(links) == 0 {
		operands = []db.Operand{
			{
				Origin: mmids.NewObjectIdFor(req.Element.GetObject()),
				Value:  0,
			},
		}
	}
	return model.StatusCompleted(NewGatherOutputState(operands))
}

////////////////////////////////////////////////////////////////////////////////
// Calculation Phase
////////////////////////////////////////////////////////////////////////////////

type CalculatePhase struct{ PhaseBase }

var _ OperatorStatePhase = (*CalculatePhase)(nil)

func (c CalculatePhase) GetCurrentState(o *OperatorState, phase Phase) model.CurrentState {
	return NewCurrentCalcState(o)
}

func (c CalculatePhase) GetTargetState(o *OperatorState, phase Phase) model.TargetState {
	return NewTargetCalcState(o)
}

func (c CalculatePhase) AcceptExternalState(lctx model.Logging, o *OperatorState, state model.ExternalStates, phase mmids.Phase) (model.AcceptStatus, error) {
	exp := o.GetDBObject().Gather.Current.ObjectVersion
	for _, s := range state {
		own := s.(*ExternalOperatorState).GetVersion()
		if own == exp {
			return c.PhaseBase.AcceptExternalState(lctx, o, state, phase)
		}
		lctx.Logger(REALM).Info("own object version {{ownvers}} does not match gather current object version {{gathervers}}", "ownvers", own, "gathervers", exp)
	}
	return model.ACCEPT_REJECTED, fmt.Errorf("gather phase not up to date")
}

func (c CalculatePhase) DBSetExternalState(log logging.Logger, o *db.OperatorState, phase Phase, state *ExternalOperatorState, mod *bool) {
	t := o.Calculation.Target
	s := state.GetState()
	support.UpdateField(&t.Operator, &s.Operator, mod)
}

func (c CalculatePhase) DBCommit(log logging.Logger, o *db.OperatorState, phase Phase, spec *model.CommitInfo, mod *bool) {
	if spec != nil {
		log.Info("  output {{output}}", "output", spec.OutputState.(*CalcOutputState).GetState())
		cc := &o.Calculation.Current
		cc.Output.Value = spec.OutputState.(*CalcOutputState).GetState()
	}
	o.Calculation.Target = nil
}

func (c CalculatePhase) Process(o *OperatorState, phase Phase, req model.Request) model.ProcessingResult {
	log := req.Logging.Logger()

	var operands []db.Operand
	for _, l := range req.Inputs {
		operands = l.(*GatherOutputState).GetState()
	}
	s := NewTargetCalcState(o)
	op := s.GetOperator()

	out := operands[0].Value
	log.Info("calculate {{operator}} {{operands}}", "operator", op, "operands", operands)
	switch op {
	case db.OP_ADD:
		for _, v := range operands[1:] {
			out += v.Value
		}
	case db.OP_SUB:
		for _, v := range operands[1:] {
			out -= v.Value
		}
	case db.OP_MUL:
		for _, v := range operands[1:] {
			out *= v.Value
		}
	case db.OP_DIV:
		for i, v := range operands[1:] {
			if v.Value == 0 {
				return model.StatusFailed(fmt.Errorf("division by zero for operand %d[%s]", i, operands[i+1].Origin))
			}
			out /= v.Value
		}
	}

	return model.StatusCompleted(NewCalcOutputState(out))
}

////////////////////////////////////////////////////////////////////////////////

type GatherOutputState = support.OutputState[[]db.Operand]
type CalcOutputState = support.OutputState[int]

var NewGatherOutputState = support.NewOutputState[[]db.Operand]
var NewCalcOutputState = support.NewOutputState[int]

////////////////////////////////////////////////////////////////////////////////

type CurrentGatherState struct {
	support.CurrentStateSupport[*db.OperatorState, *db.GatherCurrentState]
}

func NewCurrentGatherState(n *OperatorState) model.CurrentState {
	return &CurrentGatherState{support.NewCurrentStateSupport[*db.OperatorState, *db.GatherCurrentState](n, mymetamodel.PHASE_GATHER)}
}

func (c *CurrentGatherState) GetLinks() []ElementId {
	var r []ElementId

	for _, o := range c.Get().Operands {
		r = append(r, mmids.NewElementId(mymetamodel.TYPE_VALUE_STATE, c.GetNamespace(), o, mymetamodel.PHASE_PROPAGATE))
	}
	return r
}

func (c *CurrentGatherState) GetOutput() model.OutputState {
	return NewGatherOutputState(c.Get().Output.Values)
}

////////////////////////////////////////////////////////////////////////////////

type TargetGatherState struct {
	support.TargetStateSupport[*db.OperatorState, *db.GatherTargetState]
}

var _ model.TargetState = (*TargetGatherState)(nil)

func NewTargetGatherState(n *OperatorState) *TargetGatherState {
	return &TargetGatherState{support.NewTargetStateSupport[*db.OperatorState, *db.GatherTargetState](n, mymetamodel.PHASE_GATHER)}
}

func (c *TargetGatherState) GetLinks() []mmids.ElementId {
	var r []ElementId

	t := c.Get()
	if t == nil {
		return nil
	}

	for _, o := range t.Spec.Operands {
		r = append(r, mmids.NewElementId(mymetamodel.TYPE_VALUE_STATE, c.GetNamespace(), o, mymetamodel.PHASE_PROPAGATE))
	}
	return r
}

func (c *TargetGatherState) GetInputVersion(inputs model.Inputs) string {
	return support.DefaultInputVersion(inputs)
}

func (c *TargetGatherState) GetOperator() db.OperatorName {
	return c.Get().Spec.Operator
}

////////////////////////////////////////////////////////////////////////////////

type CurrentCalcState struct {
	support.CurrentStateSupport[*db.OperatorState, *db.CalculationCurrentState]
}

func NewCurrentCalcState(n *OperatorState) model.CurrentState {
	return &CurrentCalcState{support.NewCurrentStateSupport[*db.OperatorState, *db.CalculationCurrentState](n, mymetamodel.PHASE_CALCULATION)}
}

func (c *CurrentCalcState) GetLinks() []ElementId {
	return []ElementId{c.PhaseLink(mymetamodel.PHASE_GATHER)}
}

func (c *CurrentCalcState) GetOutput() model.OutputState {
	return NewCalcOutputState(c.Get().Output.Value)
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

func (c *TargetCalcState) GetInputVersion(inputs model.Inputs) string {
	return support.DefaultInputVersion(inputs)
}

func (c *TargetCalcState) GetOperator() db.OperatorName {
	return c.Get().Operator
}
