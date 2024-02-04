package explicit

import (
	"fmt"
	"reflect"

	db2 "github.com/mandelsoft/engine/pkg/impl/metamodels/valopdemo/explicit/db"
	"github.com/mandelsoft/engine/pkg/metamodel/common"
	"github.com/mandelsoft/engine/pkg/metamodel/model"
	"github.com/mandelsoft/engine/pkg/metamodel/model/support"
	"github.com/mandelsoft/engine/pkg/metamodel/objectbase/wrapped"
	"github.com/mandelsoft/engine/pkg/runtime"
	"github.com/mandelsoft/logging"

	mymetamodel "github.com/mandelsoft/engine/pkg/metamodels/valopdemo"
)

func init() {
	wrapped.MustRegisterType[OperatorState](scheme)
}

type OperatorState struct {
	support.InternalPhaseObjectSupport[*OperatorState, *db2.OperatorState, *ExternalOperatorState]
}

var _ runtime.InitializedObject = (*OperatorState)(nil)

func (n *OperatorState) Initialize() error {
	return support.SetSelf(n, nodeStatePhases)
}

var _ model.InternalObject = (*OperatorState)(nil)

var nodeStatePhases = support.NewPhases[*OperatorState, *db2.OperatorState, *ExternalOperatorState](REALM)

func init() {
	nodeStatePhases.Register(mymetamodel.PHASE_GATHER, GatherPhase{})
	nodeStatePhases.Register(mymetamodel.PHASE_CALCULATION, CalculatePhase{})
}

type Phase = support.Phase[*OperatorState, *db2.OperatorState, *ExternalOperatorState]

////////////////////////////////////////////////////////////////////////////////

type PhaseBase struct{}

func (c PhaseBase) setExternalObjectState(log logging.Logger, o *db2.OperatorState, state *ExternalOperatorState, mod *bool) {
	t := o.Target
	if t != nil {
		return // keep state from first touched phase
	}
	log.Info("set common target state for OperatorState {{name}}")
	t = &db2.ObjectTargetState{}

	s := state.GetState()
	m := !reflect.DeepEqual(t.Spec, *s) || t.ObjectVersion != state.GetVersion()
	if m {
		t.Spec = *s
		t.ObjectVersion = state.GetVersion()
	}
	*mod = *mod || m

	o.Target = t
}

func (g PhaseBase) Validate(o *OperatorState) error {
	s := TargetGatherState{o}

	op := s.GetOperator()
	if op == "" {
		return fmt.Errorf("operator missing")
	}

	if len(s.GetLinks()) == 0 {
		return fmt.Errorf("operator node requires at least one operand")
	}
	switch op {
	case db2.OP_ADD:
	case db2.OP_SUB:
	case db2.OP_DIV:
	case db2.OP_MUL:
	default:
		return fmt.Errorf("unknown operator %q", op)
	}
	return nil
}

////////////////////////////////////////////////////////////////////////////////
// Gather Phase
////////////////////////////////////////////////////////////////////////////////

type GatherPhase struct{ PhaseBase }

var _ Phase = (*GatherPhase)(nil)

func (g GatherPhase) GetCurrentState(o *OperatorState, phase model.Phase) model.CurrentState {
	return &CurrentGatherState{o}
}

func (g GatherPhase) GetTargetState(o *OperatorState, phase model.Phase) model.TargetState {
	return &TargetGatherState{o}
}

func (g GatherPhase) DBSetExternalState(log logging.Logger, o *db2.OperatorState, phase model.Phase, state *ExternalOperatorState, mod *bool) {
	g.setExternalObjectState(log, o, state, mod)
	t := o.Gather.Target
	if t == nil {
		t = &db2.GatherTargetState{}
	}

	log.Info("set target state for phase {{phase}} of OperatorState {{name}}")
	support.UpdateField(&t.ObjectVersion, &o.Target.ObjectVersion, mod)
	o.Gather.Target = t
}

func (g GatherPhase) DBCommit(log logging.Logger, o *db2.OperatorState, phase model.Phase, spec *model.CommitInfo, mod *bool) {
	if o.Gather.Target != nil && spec != nil {
		// update phase specific state
		log.Info("commit phase {{phase}} for OperatorState {{name}}")
		log.Info("  input version {{inpvers}}", "inpvers", spec.InputVersion)
		log.Info("  object version {{objvers}}", "objvers", o.Gather.Target.ObjectVersion)
		log.Info("  output version {{outvers}}", "outvers", spec.State.(*GatherOutputState).GetOutputVersion())
		log.Info("  output {{output}}", "output", spec.State.(*GatherOutputState).GetState())
		c := &o.Gather.Current
		c.InputVersion = spec.InputVersion
		c.ObjectVersion = o.Gather.Target.ObjectVersion
		c.OutputVersion = spec.State.(*GatherOutputState).GetOutputVersion()
		c.Output.Values = spec.State.(*GatherOutputState).GetState()
	}
	o.Gather.Target = nil
}

func (g GatherPhase) Process(o *OperatorState, phase model.Phase, req model.Request) model.Status {
	log := req.Logging.Logger()

	err := g.Validate(o)
	if err != nil {
		return model.Status{
			Status: common.STATUS_FAILED, // final failure
			Error:  err,
		}
	}

	links := (&TargetGatherState{o}).GetLinks()
	operands := make([]db2.Operand, len(links))
	for iid, e := range req.Inputs {
		s := e.(*ValueOutputState).GetState()
		for i, oid := range links {
			if iid == oid {
				operands[i] = db2.Operand{
					Origin: iid.ObjectId(),
					Value:  s.Value,
				}
				log.Info("found operand {{index}} from {{link}}: {{value}}", "index", i, "link", iid, "value", operands[i].Value)
				break
			}
		}
	}

	if len(links) == 0 {
		operands = []db2.Operand{
			{
				Origin: common.NewObjectIdFor(req.Element.GetObject()),
				Value:  0,
			},
		}
	}
	return model.Status{
		Status:      common.STATUS_COMPLETED,
		ResultState: NewGatherOutputState(operands),
	}
}

////////////////////////////////////////////////////////////////////////////////
// Calculation Phase
////////////////////////////////////////////////////////////////////////////////

type CalculatePhase struct{ PhaseBase }

var _ Phase = (*CalculatePhase)(nil)

func (c CalculatePhase) GetCurrentState(o *OperatorState, phase model.Phase) model.CurrentState {
	return &CurrentCalcState{o}
}

func (c CalculatePhase) GetTargetState(o *OperatorState, phase model.Phase) model.TargetState {
	return &TargetCalcState{o}
}

func (c CalculatePhase) DBSetExternalState(log logging.Logger, o *db2.OperatorState, phase model.Phase, state *ExternalOperatorState, mod *bool) {
	c.setExternalObjectState(log, o, state, mod)
	t := o.Calculation.Target
	if t == nil {
		t = &db2.CalculationTargetState{}
	}

	log.Info("set target state for phase {{phase}} of NodeState {{name}}")
	support.UpdateField(&t.ObjectVersion, &o.Target.ObjectVersion, mod)
	o.Calculation.Target = t
}

func (c CalculatePhase) DBCommit(log logging.Logger, o *db2.OperatorState, phase model.Phase, spec *model.CommitInfo, mod *bool) {
	if o.Calculation.Target != nil && spec != nil {
		// update state specific
		log.Info("commit phase {{phase}} for OperatorState {{name}}")
		log.Info("  input version {{inpvers}}", "inpvers", spec.InputVersion)
		log.Info("  object version {{objvers}}", "objvers", o.Calculation.Target.ObjectVersion)
		log.Info("  output version {{outvers}}", "outvers", spec.State.(*CalcOutputState).GetOutputVersion())
		log.Info("  output {{output}}", "output", spec.State.(*CalcOutputState).GetState())
		c := &o.Calculation.Current
		c.InputVersion = spec.InputVersion
		c.ObjectVersion = o.Calculation.Target.ObjectVersion
		c.OutputVersion = spec.State.(*CalcOutputState).GetOutputVersion()
		c.Output.Value = spec.State.(*CalcOutputState).GetState()

		// ... and common state for last phase
		log.Info("  operands {{operands}}", "operands", o.Target.Spec.Operands)
		o.Current.Operands = o.Target.Spec.Operands
	}
	o.Calculation.Target = nil
	o.Target = nil
}

func (c CalculatePhase) Process(o *OperatorState, phase model.Phase, req model.Request) model.Status {
	log := req.Logging.Logger()

	err := c.Validate(o)
	if err != nil {
		return model.Status{
			Status:      common.STATUS_FAILED, // final failure
			ResultState: nil,
			Error:       err,
		}
	}

	var operands []db2.Operand
	for _, l := range req.Inputs {
		operands = l.(*GatherOutputState).GetState()
	}
	s := (&TargetCalcState{o})
	op := s.GetOperator()

	out := operands[0].Value
	log.Info("calculate {{operator}} {{operands}}", "operator", op, "operands", operands)
	switch op {
	case db2.OP_ADD:
		for _, v := range operands[1:] {
			out += v.Value
		}
	case db2.OP_SUB:
		for _, v := range operands[1:] {
			out -= v.Value
		}
	case db2.OP_MUL:
		for _, v := range operands[1:] {
			out *= v.Value
		}
	case db2.OP_DIV:
		for i, v := range operands[1:] {
			if v.Value == 0 {
				return model.Status{
					Status: common.STATUS_FAILED,
					Error:  fmt.Errorf("division by zero for operand %d[%s]", i, operands[i+1].Origin),
				}
			}
			out /= v.Value
		}
	}

	return model.Status{
		Status:      common.STATUS_COMPLETED,
		ResultState: NewCalcOutputState(out),
	}
}

////////////////////////////////////////////////////////////////////////////////

type GatherOutputState = support.OutputState[[]db2.Operand]
type CalcOutputState = support.OutputState[int]

var NewGatherOutputState = support.NewOutputState[[]db2.Operand]
var NewCalcOutputState = support.NewOutputState[int]

////////////////////////////////////////////////////////////////////////////////

type CurrentGatherState struct {
	n *OperatorState
}

var _ model.CurrentState = (*CurrentGatherState)(nil)

func (c *CurrentGatherState) get() *db2.GatherCurrentState {
	return &c.n.GetBase().(*db2.OperatorState).Gather.Current
}

func (c *CurrentGatherState) GetLinks() []model.ElementId {
	var r []model.ElementId

	for _, o := range c.n.GetBase().(*db2.OperatorState).Current.Operands {
		r = append(r, common.NewElementId(mymetamodel.TYPE_VALUE_STATE, c.n.GetNamespace(), o, mymetamodel.PHASE_PROPAGATE))
	}
	return r
}

func (c *CurrentGatherState) GetInputVersion() string {
	return c.get().InputVersion
}

func (c *CurrentGatherState) GetObjectVersion() string {
	return c.get().ObjectVersion
}

func (c *CurrentGatherState) GetOutputVersion() string {
	return c.get().OutputVersion
}

func (c *CurrentGatherState) GetOutput() model.OutputState {
	return NewGatherOutputState(c.get().Output.Values)
}

type CurrentCalcState struct {
	n *OperatorState
}

var _ model.CurrentState = (*CurrentCalcState)(nil)

func (c *CurrentCalcState) get() *db2.CalculationCurrentState {
	return &c.n.GetBase().(*db2.OperatorState).Calculation.Current
}

func (c *CurrentCalcState) GetLinks() []model.ElementId {
	return []model.ElementId{common.NewElementId(c.n.GetType(), c.n.GetNamespace(), c.n.GetName(), mymetamodel.PHASE_GATHER)}
}

func (c *CurrentCalcState) GetInputVersion() string {
	return c.get().InputVersion
}

func (c *CurrentCalcState) GetObjectVersion() string {
	return c.get().ObjectVersion
}

func (c *CurrentCalcState) GetOutputVersion() string {
	return c.get().OutputVersion
}

func (c *CurrentCalcState) GetOutput() model.OutputState {
	return NewCalcOutputState(c.get().Output.Value)
}

////////////////////////////////////////////////////////////////////////////////

type TargetGatherState struct {
	n *OperatorState
}

var _ model.TargetState = (*TargetGatherState)(nil)

func (c *TargetGatherState) get() *db2.GatherTargetState {
	return c.n.GetBase().(*db2.OperatorState).Gather.Target
}

func (c *TargetGatherState) GetLinks() []common.ElementId {
	var r []model.ElementId

	t := c.n.GetBase().(*db2.OperatorState).Target
	if t == nil {
		return nil
	}

	for _, o := range t.Spec.Operands {
		r = append(r, common.NewElementId(mymetamodel.TYPE_VALUE_STATE, c.n.GetNamespace(), o, mymetamodel.PHASE_PROPAGATE))
	}
	return r
}

func (c *TargetGatherState) GetObjectVersion() string {
	return c.get().ObjectVersion
}

func (c *TargetGatherState) GetInputVersion(inputs model.Inputs) string {
	return support.DefaultInputVersion(inputs)
}

func (c *TargetGatherState) GetOperator() db2.OperatorName {
	return c.n.GetBase().(*db2.OperatorState).Target.Spec.Operator
}

type TargetCalcState struct {
	n *OperatorState
}

var _ model.TargetState = (*TargetCalcState)(nil)

func (c *TargetCalcState) get() *db2.CalculationTargetState {
	return c.n.GetBase().(*db2.OperatorState).Calculation.Target
}

func (c *TargetCalcState) GetLinks() []common.ElementId {
	return []model.ElementId{common.NewElementId(c.n.GetType(), c.n.GetNamespace(), c.n.GetName(), mymetamodel.PHASE_GATHER)}
}

func (c *TargetCalcState) GetObjectVersion() string {
	return c.get().ObjectVersion
}

func (c *TargetCalcState) GetInputVersion(inputs model.Inputs) string {
	return support.DefaultInputVersion(inputs)
}

func (c *TargetCalcState) GetOperator() db2.OperatorName {
	return c.n.GetBase().(*db2.OperatorState).Target.Spec.Operator
}
