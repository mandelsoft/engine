package foreigndemo

import (
	"fmt"

	"github.com/mandelsoft/engine/pkg/database"
	"github.com/mandelsoft/engine/pkg/processing/metamodel/objectbase"
	. "github.com/mandelsoft/engine/pkg/processing/mmids"

	"github.com/mandelsoft/engine/pkg/processing/metamodel/objectbase/wrapped"
	"github.com/mandelsoft/engine/pkg/processing/mmids"
	"github.com/mandelsoft/engine/pkg/processing/model"
	"github.com/mandelsoft/engine/pkg/processing/model/support"
	"github.com/mandelsoft/engine/pkg/runtime"
	"github.com/mandelsoft/engine/pkg/utils"
	"github.com/mandelsoft/logging"

	"github.com/mandelsoft/engine/pkg/impl/metamodels/foreigndemo/db"
	mymetamodel "github.com/mandelsoft/engine/pkg/metamodels/foreigndemo"
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

		for n := range s.Operations {
			if _, ok := s.Operands[n]; ok {
				return model.ACCEPT_INVALID, fmt.Errorf("operands name %q also used as operation name", n)
			}
		}
		for n, e := range s.Operations {
			op := e.Operator
			if op == "" {
				return model.ACCEPT_INVALID, fmt.Errorf("operator missing for operation %q", n)
			}

			switch op {
			case db.OP_ADD:
			case db.OP_SUB:
			case db.OP_DIV:
			case db.OP_MUL:
			default:
				return model.ACCEPT_INVALID, fmt.Errorf("unknown operator %q for operation %q", op, n)
			}

			for i, c := range e.Operands {
				if _, ok := s.Operands[c]; !ok {
					return model.ACCEPT_INVALID, fmt.Errorf("operand %d of operation %q: unknown input %q", i+1, n, c)
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

func (g GatherPhase) GetCurrentState(o *OperatorState, phase Phase) model.CurrentState {
	return NewCurrentGatherState(o)
}

func (g GatherPhase) GetTargetState(o *OperatorState, phase Phase) model.TargetState {
	return NewTargetGatherState(o)
}

func (_ GatherPhase) DBSetExternalState(log logging.Logger, o *db.OperatorState, phase Phase, state *ExternalOperatorState, mod *bool) {
	t := o.Gather.Target

	log.Info("set target state for phase {{phase}} of OperatorState {{name}}")
	support.UpdateField(&t.Spec, state.GetState(), mod)
}

func (_ GatherPhase) DBCommit(log logging.Logger, o *db.OperatorState, phase Phase, spec *model.CommitInfo, mod *bool) {
	if o.Gather.Target != nil && spec != nil {
		// update phase specific state
		log.Info("  output {{output}}", "output", spec.OutputState.(*GatherOutputState).GetState())
		c := &o.Gather.Current
		support.UpdateField(&c.Output, spec.OutputState.(*GatherOutputState).GetState(), mod)
	} else {
		log.Info("nothing to commit for phase {{phase}} of OperatorState {{name}}")
	}
}

func (_ GatherPhase) Process(o *OperatorState, phase Phase, req model.Request) model.ProcessingResult {
	log := req.Logging.Logger()

	if req.Delete {
		log.Info("deletion successful")
		return model.StatusDeleted()
	}
	t := NewTargetGatherState(o)
	operands := map[string]db.OperandInfo{}
	for iid, e := range req.Inputs {
		s := e.(*ValueOutputState).GetState()
		for n, src := range t.GetOperands() {
			if iid.GetName() == src {
				operands[n] = db.OperandInfo{
					Origin: iid.ObjectId(),
					Value:  s.Value,
				}
				log.Info("found operand {{name}} from {{link}}: {{value}}", "name", n, "link", iid, "value", s.Value)
				break
			}
		}
	}

	// check target expression object.
	err := req.SlaveManagement.AssureSlaves(
		nil,
		support.SlaveCreationOnly,
		model.SlaveId(req.Element.Id(), mymetamodel.TYPE_EXPRESSION_STATE, mymetamodel.PHASE_EVALUATION),
	)

	if err != nil {
		return model.StatusCompleted(nil, err)
	}

	out := &db.GatherOutput{
		Operands:   operands,
		Operations: t.GetOperations(),
	}
	return model.StatusCompleted(NewGatherOutputState(out))
}

func (_ GatherPhase) PrepareDeletion(log logging.Logger, ob objectbase.Objectbase, o *OperatorState, phase mmids.Phase) error {
	oid := model.SlaveObjectId(o, mymetamodel.TYPE_EXPRESSION)
	return support.RequestSlaveDeletion(log, ob, oid)
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

func (c CalculatePhase) AcceptExternalState(log logging.Logger, o *OperatorState, state model.ExternalStates, phase mmids.Phase) (model.AcceptStatus, error) {
	exp := o.GetDBObject().Gather.Current.ObjectVersion
	for _, s := range state {
		own := s.(*ExternalOperatorState).GetVersion()
		if own == exp {
			return c.PhaseBase.AcceptExternalState(log, o, state, phase)
		}
		log.Info("own object version {{ownvers}} does not match gather current object version {{gathervers}}", "ownvers", own, "gathervers", exp)
	}
	return model.ACCEPT_REJECTED, fmt.Errorf("gather phase not up to date")
}

func (c CalculatePhase) DBSetExternalState(log logging.Logger, o *db.OperatorState, phase Phase, state *ExternalOperatorState, mod *bool) {
	log.Info("set target state for phase {{phase}} of Operator {{name}}")
	t := o.Calculation.Target
	s := state.GetState()
	support.UpdateField(&t.Operands, &s.Operands, mod)
	support.UpdateField(&t.Outputs, &s.Outputs, mod)
}

func (c CalculatePhase) DBCommit(log logging.Logger, o *db.OperatorState, phase Phase, spec *model.CommitInfo, mod *bool) {
	if o.Calculation.Target != nil && spec != nil {
		c := &o.Calculation.Current
		log.Info("  output {{output}}", "output", spec.OutputState.(*CalcOutputState).GetState())
		support.UpdateField(&c.Output, utils.Pointer(spec.OutputState.(*CalcOutputState).GetState()), mod)
	} else {
		log.Info("nothing to commit for phase {{phase}} of OperatorState {{name}}")
	}
}

func (c CalculatePhase) Process(o *OperatorState, phase Phase, req model.Request) model.ProcessingResult {
	log := req.Logging.Logger()

	if req.Delete {
		log.Info("deletion successful")
		return model.StatusDeleted()
	}
	s := NewTargetCalcState(o)

	out := db.CalculationOutput{}
	values := map[string]int{}

	log.Info("preparing effective value set")
	op := req.Inputs[s.PhaseLink(mymetamodel.PHASE_GATHER)].(*GatherOutputState).GetState()
	for n, o := range op.Operands {
		values[n] = o.Value
	}
	log.Info("- value set from inputs: {{values}}", "values", values)
	ex := req.Inputs[s.SlaveLink(mymetamodel.TYPE_EXPRESSION_STATE, mymetamodel.PHASE_EVALUATION)].(*EvaluationOutputState).GetState()
	log.Info("- value set from expressions: {{values}}", "values", ex)
	for n, v := range ex {
		values[n] = v
	}

	// calculate value schedule
	log.Info("preparing outbound assignments")
	for i, e := range s.GetOutputs() {
		v := values[e]
		out[i] = v
		log.Info("- {{outbound}}: {{value}}", "outbound", i, "value", v)
	}

	var slaves []ElementId
	for k := range s.GetOutputs() {
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

	return model.StatusCompleted(NewCalcOutputState(out))
}

func (_ CalculatePhase) PrepareDeletion(log logging.Logger, ob objectbase.Objectbase, o *OperatorState, phase mmids.Phase) error {
	s := NewCurrentCalcState(o)

	for k := range s.GetOutput().(*CalcOutputState).GetState() {
		oid := database.NewObjectId(mymetamodel.TYPE_VALUE, o.GetNamespace(), k)
		err := support.RequestSlaveDeletion(log, ob, oid)
		if err != nil {
			return err
		}
	}
	return nil
}

////////////////////////////////////////////////////////////////////////////////

type GatherOutputState = support.OutputState[*db.GatherOutput]
type CalcOutputState = support.OutputState[db.CalculationOutput]

var NewGatherOutputState = support.NewOutputState[*db.GatherOutput]
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

	for _, o := range c.Get().Output.Operands {
		r = append(r, mmids.NewElementId(mymetamodel.TYPE_VALUE_STATE, o.Origin.GetNamespace(), o.Origin.GetName(), mymetamodel.PHASE_PROPAGATE))
	}
	return r
}

func (c *CurrentGatherState) GetOutput() model.OutputState {
	return NewGatherOutputState(&c.Get().Output)
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

func (c *TargetGatherState) GetOperations() map[string]db.Operation {
	return c.Get().Spec.Operations
}

func (c *TargetGatherState) GetOperands() map[string]string {
	return c.Get().Spec.Operands
}

////////////////////////////////////////////////////////////////////////////////

type CurrentCalcState struct {
	support.CurrentStateSupport[*db.OperatorState, *db.CalculationCurrentState]
}

func NewCurrentCalcState(n *OperatorState) model.CurrentState {
	return &CurrentCalcState{support.NewCurrentStateSupport[*db.OperatorState, *db.CalculationCurrentState](n, mymetamodel.PHASE_CALCULATION)}
}

func (c *CurrentCalcState) GetLinks() []ElementId {
	return []ElementId{c.PhaseLink(mymetamodel.PHASE_GATHER), c.SlaveLink(mymetamodel.TYPE_EXPRESSION_STATE, mymetamodel.PHASE_EVALUATION)}
}

func (c *CurrentCalcState) GetOutput() model.OutputState {
	return NewCalcOutputState(c.Get().Output)
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
	return []ElementId{c.PhaseLink(mymetamodel.PHASE_GATHER), c.SlaveLink(mymetamodel.TYPE_EXPRESSION_STATE, mymetamodel.PHASE_EVALUATION)}
}

func (c *TargetCalcState) GetOperands() map[string]string {
	return c.Get().Operands
}

func (c *TargetCalcState) GetOutputs() map[string]string {
	return c.Get().Outputs
}
