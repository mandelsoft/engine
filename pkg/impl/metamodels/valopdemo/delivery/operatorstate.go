package delivery

import (
	"fmt"

	. "github.com/mandelsoft/engine/pkg/processing/mmids"
	"github.com/mandelsoft/engine/pkg/processing/objectbase/wrapped"

	"github.com/mandelsoft/engine/pkg/processing/mmids"
	"github.com/mandelsoft/engine/pkg/processing/model"
	"github.com/mandelsoft/engine/pkg/processing/model/support"
	"github.com/mandelsoft/engine/pkg/runtime"
	"github.com/mandelsoft/engine/pkg/utils"
	"github.com/mandelsoft/logging"

	"github.com/mandelsoft/engine/pkg/impl/metamodels/valopdemo/delivery/db"
	mymetamodel "github.com/mandelsoft/engine/pkg/metamodels/valopdemo"
)

func init() {
	wrapped.MustRegisterType[OperatorState](scheme)
}

type OperatorState struct {
	support.InternalPhaseObjectSupport[*OperatorState, *db.OperatorState, *ExternalOperatorState]
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

func (_ PhaseBase) AcceptExternalState(log logging.Logger, o *OperatorState, state model.ExternalStates, phase mmids.Phase) (model.AcceptStatus, error) {
	for _, _s := range state {
		s := _s.(*ExternalOperatorState).GetState()

		if len(s.Operands) == 0 {
			return model.ACCEPT_INVALID, fmt.Errorf("operator node requires at least one operand")
		}

		for i, e := range s.Operations {
			op := e.Operator
			if op == "" {
				return model.ACCEPT_INVALID, fmt.Errorf("operator missing for operation %d", i+1)
			}

			switch op {
			case db.OP_ADD:
			case db.OP_SUB:
			case db.OP_DIV:
			case db.OP_MUL:
			default:
				return model.ACCEPT_INVALID, fmt.Errorf("unknown operator %q for operation %d", op, i+1)
			}

			for j, c := range s.Operations {
				if i != j && c.Target == e.Target {
					return model.ACCEPT_INVALID, fmt.Errorf("operation %d and %d use the same target (%s)", i+1, j+1, c.Target)
				}
			}
		}
	}
	return model.ACCEPT_OK, nil
}

////////////////////////////////////////////////////////////////////////////////
// Gather Phase
////////////////////////////////////////////////////////////////////////////////

type GatherPhase struct{ PhaseBase }

var _ OperatorStatePhase = (*GatherPhase)(nil)

func (_ GatherPhase) GetCurrentState(o *OperatorState, phase Phase) model.CurrentState {
	return NewCurrentGatherState(o)
}

func (_ GatherPhase) GetTargetState(o *OperatorState, phase Phase) model.TargetState {
	return NewTargetGatherState(o)
}

func (_ GatherPhase) DBSetExternalState(log logging.Logger, o *db.OperatorState, phase Phase, state *ExternalOperatorState, mod *bool) {
	t := o.Gather.Target

	log.Info("set target state for phase {{phase}} of OperatorState {{name}}")
	support.UpdateField(&t.Spec, state.GetState(), mod)
}

func (p CalculatePhase) AcceptExternalState(log logging.Logger, o *OperatorState, state model.ExternalStates, phase mmids.Phase) (model.AcceptStatus, error) {
	exp := o.GetDBObject().Gather.Current.ObjectVersion
	for _, s := range state {
		own := s.(*ExternalOperatorState).GetVersion()
		if own == exp {
			return p.PhaseBase.AcceptExternalState(log, o, state, phase)
		}
		log.Info("own object version {{ownvers}} does not match gather current object version {{gathervers}}", "ownvers", own, "gathervers", exp)
	}
	return model.ACCEPT_REJECTED, fmt.Errorf("gather phase not up to date")
}

func (g GatherPhase) DBCommit(log logging.Logger, o *db.OperatorState, phase Phase, spec *model.CommitInfo, mod *bool) {
	if o.Gather.Target != nil && spec != nil {
		// update phase specific state
		log.Info("  output {{output}}", "output", spec.OutputState.(*GatherOutputState).GetState())
		c := &o.Gather.Current
		c.Output.Values = spec.OutputState.(*GatherOutputState).GetState()
	} else {
		log.Info("nothing to commit for phase {{phase}} of OperatorState {{name}}")
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
	return model.StatusCompleted(NewGatherOutputState(req.FormalVersion, operands))
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

func (c CalculatePhase) DBSetExternalState(log logging.Logger, o *db.OperatorState, phase Phase, state *ExternalOperatorState, mod *bool) {
	log.Info("set target state for phase {{phase}} of Operator {{name}}")
	t := o.Calculation.Target
	s := state.GetState()
	support.UpdateField(&t.Operations, &s.Operations, mod)
}

func (c CalculatePhase) DBCommit(log logging.Logger, o *db.OperatorState, phase Phase, spec *model.CommitInfo, mod *bool) {
	if o.Calculation.Target != nil && spec != nil {
		c := &o.Calculation.Current
		log.Info("  output {{output}}", "output", spec.OutputState.(*CalcOutputState).GetState())
		c.Output = spec.OutputState.(*CalcOutputState).GetState()
	} else {
		log.Info("nothing to commit for phase {{phase}} of OperatorState {{name}}")
	}
}

func (c CalculatePhase) Process(o *OperatorState, phase Phase, req model.Request) model.ProcessingResult {
	log := req.Logging.Logger()

	var operands []db.Operand
	for _, l := range req.Inputs {
		operands = l.(*GatherOutputState).GetState()
	}
	s := NewTargetCalcState(o)

	// calculate target values from operation definitions
	r := db.CalculationOutput{}
	for i, e := range s.GetOperations() {
		op := e.Operator

		out := operands[0].Value
		log.Info("calculate operation {{operation}}: {{operator}} {{operands}}", "operation", i+1, "operator", op, "operands", operands)
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
		r[e.Target] = out
	}

	// check target value objects.
	var slaves []ElementId
	for k := range r {
		slaves = append(slaves, NewElementId(mymetamodel.TYPE_VALUE_STATE, req.Element.GetNamespace(), k, mymetamodel.PHASE_PROPAGATE))
	}

	req.SlaveManagement.AssureSlaves(
		func(i model.InternalObject) error {
			o := i.(support.InternalObject).GetBase().(*db.ValueState)
			if o.Spec.Provider != "" && o.Spec.Provider != req.Element.GetName() {
				return fmt.Errorf("target value object %q already served by operatpr %q", i.GetName(), req.Element.GetName())
			}
			return nil
		},
		support.SlaveCreationFunc(func(o *db.ValueState) (bool, bool) {
			mod := false
			support.UpdateField(&o.Spec.Provider, utils.Pointer(req.Element.GetName()), &mod)
			return mod, mod
		}),
		slaves...,
	)

	return model.StatusCompleted(NewCalcOutputState(req.FormalVersion, r))
}

////////////////////////////////////////////////////////////////////////////////

type GatherOutputState = support.OutputState[[]db.Operand]
type CalcOutputState = support.OutputState[db.CalculationOutput]

var NewGatherOutputState = support.NewOutputState[[]db.Operand]
var NewCalcOutputState = support.NewOutputState[db.CalculationOutput]

////////////////////////////////////////////////////////////////////////////////

type CurrentGatherState struct {
	support.CurrentStateSupport[*db.OperatorState, *db.GatherCurrentState]
}

func NewCurrentGatherState(n *OperatorState) model.CurrentState {
	return &CurrentGatherState{support.NewCurrentStateSupport[*db.OperatorState, *db.GatherCurrentState](n, mymetamodel.PHASE_GATHER)}
}

var _ model.CurrentState = (*CurrentGatherState)(nil)

func (c *CurrentGatherState) GetLinks() []ElementId {
	var r []ElementId

	for _, o := range c.Get().Operands {
		r = append(r, mmids.NewElementId(mymetamodel.TYPE_VALUE_STATE, c.GetNamespace(), o, mymetamodel.PHASE_PROPAGATE))
	}
	return r
}

func (c *CurrentGatherState) GetOutput() model.OutputState {
	return NewGatherOutputState(c.GetFormalVersion(), c.Get().Output.Values)
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

func (c *TargetGatherState) GetOperations() []db.Operation {
	return c.Get().Spec.Operations
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
	return NewCalcOutputState(c.GetFormalVersion(), c.Get().Output)
}

////////////////////////////////////////////////////////////////////////////////

type TargetCalcState struct {
	support.TargetStateSupport[*db.OperatorState, *db.CalculationTargetState]
}

var _ model.TargetState = (*TargetCalcState)(nil)

func NewTargetCalcState(n *OperatorState) *TargetCalcState {
	return &TargetCalcState{support.NewTargetStateSupport[*db.OperatorState, *db.CalculationTargetState](n, mymetamodel.PHASE_CALCULATION)}
}

func (c *TargetCalcState) GetLinks() []mmids.ElementId {
	return []ElementId{c.PhaseLink(mymetamodel.PHASE_GATHER)}
}

func (c *TargetCalcState) GetOperations() []db.Operation {
	return c.Get().Operations
}
