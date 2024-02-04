package delivery

import (
	"fmt"
	"reflect"

	. "github.com/mandelsoft/engine/pkg/processing/mmids"

	"github.com/mandelsoft/engine/pkg/processing/metamodel/model"
	"github.com/mandelsoft/engine/pkg/processing/metamodel/model/support"
	"github.com/mandelsoft/engine/pkg/processing/metamodel/objectbase/wrapped"
	"github.com/mandelsoft/engine/pkg/processing/mmids"
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
	return support.SetSelf(n, nodeStatePhases)
}

var _ model.InternalObject = (*OperatorState)(nil)

var nodeStatePhases = support.NewPhases[*OperatorState, *db.OperatorState, *ExternalOperatorState](REALM)

func init() {
	nodeStatePhases.Register(mymetamodel.PHASE_GATHER, GatherPhase{})
	nodeStatePhases.Register(mymetamodel.PHASE_CALCULATION, CalculatePhase{})
}

type OperatorStatePhase = support.Phase[*OperatorState, *db.OperatorState, *ExternalOperatorState]

////////////////////////////////////////////////////////////////////////////////

type PhaseBase struct{}

func (c PhaseBase) setExternalObjectState(log logging.Logger, o *db.OperatorState, state *ExternalOperatorState, mod *bool) {
	t := o.Target
	if t != nil {
		return // keep state from first touched phase
	}
	log.Info("set common target state for OperatorState {{name}}")
	t = &db.ObjectTargetState{}

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

	if len(s.GetLinks()) == 0 {
		return fmt.Errorf("operator node requires at least one operand")
	}

	for i, e := range s.GetOperations() {
		op := e.Operator
		if op == "" {
			return fmt.Errorf("operator missing for operation %d", i+1)
		}

		switch op {
		case db.OP_ADD:
		case db.OP_SUB:
		case db.OP_DIV:
		case db.OP_MUL:
		default:
			return fmt.Errorf("unknown operator %q for operation %d", op, i+1)
		}

		for j, c := range s.GetOperations() {
			if i != j && c.Target == e.Target {
				return fmt.Errorf("operation %d and %d use the same target (%s)", i+1, j+1, c.Target)
			}
		}
	}
	return nil
}

////////////////////////////////////////////////////////////////////////////////
// Gather Phase
////////////////////////////////////////////////////////////////////////////////

type GatherPhase struct{ PhaseBase }

var _ OperatorStatePhase = (*GatherPhase)(nil)

func (g GatherPhase) GetCurrentState(o *OperatorState, phase Phase) model.CurrentState {
	return &CurrentGatherState{o}
}

func (g GatherPhase) GetTargetState(o *OperatorState, phase Phase) model.TargetState {
	return &TargetGatherState{o}
}

func (g GatherPhase) DBSetExternalState(log logging.Logger, o *db.OperatorState, phase Phase, state *ExternalOperatorState, mod *bool) {
	g.setExternalObjectState(log, o, state, mod)
	t := o.Gather.Target
	if t == nil {
		t = &db.GatherTargetState{}
	}

	log.Info("set target state for phase {{phase}} of OperatorState {{name}}")
	support.UpdateField(&t.ObjectVersion, &o.Target.ObjectVersion, mod)
	o.Gather.Target = t
}

func (g GatherPhase) DBCommit(log logging.Logger, o *db.OperatorState, phase Phase, spec *model.CommitInfo, mod *bool) {
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
	} else {
		log.Info("nothing to commit for phase {{phase}} of OperatorState {{name}}")
	}
	o.Gather.Target = nil
}

func (g GatherPhase) Process(o *OperatorState, phase Phase, req model.Request) model.Status {
	log := req.Logging.Logger()

	err := g.Validate(o)
	if err != nil {
		return model.StatusFailed(err)
	}

	links := (&TargetGatherState{o}).GetLinks()
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
	return &CurrentCalcState{o}
}

func (c CalculatePhase) GetTargetState(o *OperatorState, phase Phase) model.TargetState {
	return &TargetCalcState{o}
}

func (c CalculatePhase) DBSetExternalState(log logging.Logger, o *db.OperatorState, phase Phase, state *ExternalOperatorState, mod *bool) {
	c.setExternalObjectState(log, o, state, mod)
	t := o.Calculation.Target
	if t == nil {
		t = &db.CalculationTargetState{}
	}

	log.Info("set target state for phase {{phase}} of NodeState {{name}}")
	support.UpdateField(&t.ObjectVersion, &o.Target.ObjectVersion, mod)
	o.Calculation.Target = t
}

func (c CalculatePhase) DBCommit(log logging.Logger, o *db.OperatorState, phase Phase, spec *model.CommitInfo, mod *bool) {
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
		c.Output = spec.State.(*CalcOutputState).GetState()

		// ... and common state for last phase
		log.Info("  operands {{operands}}", "operands", o.Target.Spec.Operands)
		o.Current.Operands = o.Target.Spec.Operands
	} else {
		log.Info("nothing to commit for phase {{phase}} of OperatorState {{name}}")
	}
	o.Calculation.Target = nil
	o.Target = nil
}

func (c CalculatePhase) Process(o *OperatorState, phase Phase, req model.Request) model.Status {
	log := req.Logging.Logger()

	err := c.Validate(o)
	if err != nil {
		return model.StatusFailed(err)
	}

	var operands []db.Operand
	for _, l := range req.Inputs {
		operands = l.(*GatherOutputState).GetState()
	}
	s := (&TargetCalcState{o})

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

	ob := req.Model.ObjectBase()

	// check target value objects.
	var creations []model.Creation
	for k := range r {
		eid := mmids.NewElementId(mymetamodel.TYPE_VALUE_STATE, req.Element.GetNamespace(), k, mymetamodel.PHASE_PROPAGATE)
		log := log.WithValues("target", k)
		log.Info("checking target {{target}}")
		t := req.ElementAccess.GetElement(eid)
		if t == nil {
			typ := mmids.NewTypeId(mymetamodel.TYPE_VALUE_STATE, mymetamodel.PHASE_PROPAGATE)
			_, i, created, err := support.AssureElement(log, ob, typ, k, req,
				func(o *db.ValueState) (bool, bool) {
					mod := false
					support.UpdateField(&o.Spec.Provider, utils.Pointer(req.Element.GetName()), &mod)
					return mod, mod
				},
			)
			if created {
				creations = append(creations, model.Creation{
					Internal: i,
					Phase:    typ.GetPhase(),
				})
			}
			if err != nil {
				return model.StatusCompletedWithCreation(creations, nil, err)
			}
		}
	}
	return model.StatusCompletedWithCreation(creations, NewCalcOutputState(r))
}

////////////////////////////////////////////////////////////////////////////////

type GatherOutputState = support.OutputState[[]db.Operand]
type CalcOutputState = support.OutputState[db.CalculationOutput]

var NewGatherOutputState = support.NewOutputState[[]db.Operand]
var NewCalcOutputState = support.NewOutputState[db.CalculationOutput]

////////////////////////////////////////////////////////////////////////////////

type CurrentGatherState struct {
	n *OperatorState
}

var _ model.CurrentState = (*CurrentGatherState)(nil)

func (c *CurrentGatherState) get() *db.GatherCurrentState {
	return &c.n.GetBase().(*db.OperatorState).Gather.Current
}

func (c *CurrentGatherState) GetLinks() []ElementId {
	var r []ElementId

	for _, o := range c.n.GetBase().(*db.OperatorState).Current.Operands {
		r = append(r, mmids.NewElementId(mymetamodel.TYPE_VALUE_STATE, c.n.GetNamespace(), o, mymetamodel.PHASE_PROPAGATE))
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

func (c *CurrentCalcState) get() *db.CalculationCurrentState {
	return &c.n.GetBase().(*db.OperatorState).Calculation.Current
}

func (c *CurrentCalcState) GetLinks() []ElementId {
	return []ElementId{mmids.NewElementId(c.n.GetType(), c.n.GetNamespace(), c.n.GetName(), mymetamodel.PHASE_GATHER)}
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
	return NewCalcOutputState(c.get().Output)
}

////////////////////////////////////////////////////////////////////////////////

type TargetGatherState struct {
	n *OperatorState
}

var _ model.TargetState = (*TargetGatherState)(nil)

func (c *TargetGatherState) get() *db.GatherTargetState {
	return c.n.GetBase().(*db.OperatorState).Gather.Target
}

func (c *TargetGatherState) GetLinks() []mmids.ElementId {
	var r []ElementId

	t := c.n.GetBase().(*db.OperatorState).Target
	if t == nil {
		return nil
	}

	for _, o := range t.Spec.Operands {
		r = append(r, mmids.NewElementId(mymetamodel.TYPE_VALUE_STATE, c.n.GetNamespace(), o, mymetamodel.PHASE_PROPAGATE))
	}
	return r
}

func (c *TargetGatherState) GetObjectVersion() string {
	return c.get().ObjectVersion
}

func (c *TargetGatherState) GetInputVersion(inputs model.Inputs) string {
	return support.DefaultInputVersion(inputs)
}

func (c *TargetGatherState) GetOperations() []db.Operation {
	return c.n.GetBase().(*db.OperatorState).Target.Spec.Operations
}

type TargetCalcState struct {
	n *OperatorState
}

var _ model.TargetState = (*TargetCalcState)(nil)

func (c *TargetCalcState) get() *db.CalculationTargetState {
	return c.n.GetBase().(*db.OperatorState).Calculation.Target
}

func (c *TargetCalcState) GetLinks() []mmids.ElementId {
	return []ElementId{mmids.NewElementId(c.n.GetType(), c.n.GetNamespace(), c.n.GetName(), mymetamodel.PHASE_GATHER)}
}

func (c *TargetCalcState) GetObjectVersion() string {
	return c.get().ObjectVersion
}

func (c *TargetCalcState) GetInputVersion(inputs model.Inputs) string {
	return support.DefaultInputVersion(inputs)
}

func (c *TargetCalcState) GetOperations() []db.Operation {
	return c.n.GetBase().(*db.OperatorState).Target.Spec.Operations
}
