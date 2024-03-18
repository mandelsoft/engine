package sub

import (
	"fmt"
	"strconv"

	. "github.com/mandelsoft/engine/pkg/processing/mmids"
	db2 "github.com/mandelsoft/engine/pkg/processing/model/support/db"
	"github.com/mandelsoft/goutils/generics"
	"github.com/mandelsoft/goutils/maputils"

	"github.com/mandelsoft/logging"

	"github.com/mandelsoft/engine/pkg/impl/metamodels/foreigndemo/sub/db"
	mymetamodel "github.com/mandelsoft/engine/pkg/metamodels/foreigndemo"
	"github.com/mandelsoft/engine/pkg/processing/model"
	"github.com/mandelsoft/engine/pkg/processing/model/support"
)

////////////////////////////////////////////////////////////////////////////////
// Gather Phase
////////////////////////////////////////////////////////////////////////////////

type GatherPhase struct{ PhaseBase }

var _ OperatorStatePhase = (*GatherPhase)(nil)

func (_ GatherPhase) AcceptExternalState(log logging.Logger, o *OperatorState, state model.ExternalState, phase Phase) (model.AcceptStatus, error) {
	s := state.(*ExternalOperatorState).GetState()

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
		case db.OP_ADD, db.OP_SUB, db.OP_DIV, db.OP_MUL:
			if e.Expression != "" {
				return model.ACCEPT_INVALID, fmt.Errorf("no expression possible for operation %q(%s)", n, op)
			}
			for i, c := range e.Operands {
				if _, err := strconv.Atoi(c); err != nil {
					if _, ok := s.Operands[c]; !ok {
						return model.ACCEPT_INVALID, fmt.Errorf("operand %d of operation %q: unknown input %q", i+1, n, c)
					}
				}
			}
		case db.OP_EXPR:
			if e.Expression == "" {
				return model.ACCEPT_INVALID, fmt.Errorf("expression required for operation %q(%s)", n, op)
			}
			if len(e.Operands) > 0 {
				return model.ACCEPT_INVALID, fmt.Errorf("no operands possible for operation %q(%s)", n, op)
			}
		default:
			return model.ACCEPT_INVALID, fmt.Errorf("unknown operator %q for operation %q", op, n)
		}

	}
	return model.ACCEPT_OK, nil
}

func (g GatherPhase) GetCurrentState(o *OperatorState, phase Phase) model.CurrentState {
	return NewCurrentGatherState(o)
}

func (g GatherPhase) GetTargetState(o *OperatorState, phase Phase) model.TargetState {
	return g.getTargetState(o)
}

func (_ GatherPhase) getTargetState(o *OperatorState) *TargetGatherState {
	return NewTargetGatherState(o)
}

func (_ GatherPhase) DBSetExternalState(log logging.Logger, o *db.OperatorState, phase Phase, state model.ExternalState, mod *bool) {
	t := o.Gather.Target

	log.Info("set target state for phase {{phase}} of OperatorState {{name}}")
	support.UpdateField(&t.Spec, state.(*ExternalOperatorState).GetState(), mod)
}

func (_ GatherPhase) DBRollback(log logging.Logger, o *db.OperatorState, phase Phase, mod *bool) {
	if o.Gather.Target != nil {
		c := &o.Gather.Current
		log.Info("  observed operands {{operands}}", "operands", o.Gather.Target.Spec.Operands)
		c.ObservedOperands = o.Gather.Target.Spec.Operands
	}
}

func (_ GatherPhase) DBCommit(log logging.Logger, o *db.OperatorState, phase Phase, spec *model.CommitInfo, mod *bool) {
	if o.Gather.Target != nil && spec != nil {
		// update phase specific state
		c := &o.Gather.Current
		log.Info("  operands {{operands}}", "operands", o.Gather.Target.Spec.Operands)
		c.ObservedOperands = o.Gather.Target.Spec.Operands
		log.Info("  output {{output}}", "output", spec.OutputState.(*GatherOutputState).GetState())
		support.UpdateField(&c.Output, spec.OutputState.(*GatherOutputState).GetState(), mod)
	} else {
		log.Info("nothing to commit for phase {{phase}} of OperatorState {{name}}")
	}
}

func (g GatherPhase) Process(o *OperatorState, phase Phase, req model.Request) model.ProcessingResult {
	log := req.Logging.Logger()

	if req.Delete {
		log.Info("deletion successful")
		return model.StatusDeleted()
	}
	t := g.getTargetState(o)
	operands := map[string]db.Operand{}
	for iid, e := range req.Inputs {
		s := e.(*ValueOutputState).GetState()
		for n, src := range t.GetOperands() {
			if iid.GetName() == src {
				operands[n] = db.Operand{
					Origin: db2.NewObjectIdFor(iid),
					Value:  s.Value,
				}
				log.Info("found operand {{name}} from {{link}}: {{value}}", "name", n, "link", iid, "value", s.Value)
				break
			}
		}
	}

	for n, src := range t.GetOperands() {
		v, err := strconv.Atoi(src)
		if err == nil {
			operands[n] = db.Operand{
				Origin: db2.NewObjectIdFor(req.Element.Id()),
				Value:  v,
			}
			log.Info("found inline operand {{name}}: {{value}}", "name", n, "value", v)
		}
	}

	// check target expression object.
	err := req.SlaveManagement.AssureSlaves(
		func(i model.InternalObject) error {
			o := i.(support.InternalObject).GetBase().(*db.ExpressionState)
			if o.Spec.Provider != "" && o.Spec.Provider != req.Element.GetName() {
				return fmt.Errorf("target expression object %q already served by operatpr %q", i.GetName(), req.Element.GetName())
			}
			return nil
		},
		support.SlaveCreationFunc(func(o *db.ExpressionState) (bool, bool) {
			mod := false
			support.UpdateField(&o.Spec.Provider, generics.Pointer(req.Element.GetName()), &mod)
			if mod {
				log.Info("update provider for {{slaveid}} to {{provider}}", "slaveid", NewElementIdForPhase(o, mymetamodel.PHASE_PROPAGATE), req.Element.GetName())
			} else {
				log.Info("preserve provider {{provider}} for {{slaveid}}", "slaveid", NewElementIdForPhase(o, mymetamodel.PHASE_PROPAGATE), req.Element.GetName())
			}
			return mod, mod
		}),
		model.SlaveId(req.Element.Id(), mymetamodel.TYPE_EXPRESSION_STATE, mymetamodel.PHASE_CALCULATE),
	)

	if err != nil {
		return model.StatusCompleted(nil, err)
	}

	out := &db.GatherOutput{
		Operands:   operands,
		Operations: t.GetOperations(),
		Outputs:    t.GetOutputs(),
	}
	return model.StatusCompleted(NewGatherOutputState(req.FormalVersion, out))
}

func (_ GatherPhase) PrepareDeletion(log logging.Logger, mgmt model.SlaveManagement, o *OperatorState, phase Phase) error {
	eid := model.SlaveId(o, mymetamodel.TYPE_EXPRESSION_STATE, mymetamodel.PHASE_CALCULATE)
	return mgmt.MarkForDeletion(eid)
}

////////////////////////////////////////////////////////////////////////////////

type GatherOutputState = support.OutputState[*db.GatherOutput]

var NewGatherOutputState = support.NewOutputState[*db.GatherOutput]

////////////////////////////////////////////////////////////////////////////////

type CurrentGatherState struct {
	support.CurrentStateSupport[*db.OperatorState, *db.GatherCurrentState]
}

func NewCurrentGatherState(n *OperatorState) model.CurrentState {
	return &CurrentGatherState{support.NewCurrentStateSupport[*db.OperatorState, *db.GatherCurrentState](n, mymetamodel.PHASE_GATHER)}
}

var _ model.CurrentState = (*CurrentGatherState)(nil)

func (c *CurrentGatherState) GetObservedState() model.ObservedState {
	if c.GetObjectVersion() == c.GetObservedVersion() {
		return c
	}
	return c.GetObservedStateForTypeAndPhase(mymetamodel.TYPE_VALUE_STATE, mymetamodel.PHASE_PROPAGATE, maputils.OrderedValues(c.Get().ObservedOperands)...)
}

func (c *CurrentGatherState) GetLinks() []ElementId {
	var r []ElementId

	for _, o := range c.Get().Output.Operands {
		r = append(r, NewElementId(mymetamodel.TYPE_VALUE_STATE, o.Origin.GetNamespace(), o.Origin.GetName(), mymetamodel.PHASE_PROPAGATE))
	}
	return r
}

func (c *CurrentGatherState) GetOutput() model.OutputState {
	return NewGatherOutputState(c.GetFormalVersion(), &c.Get().Output)
}

////////////////////////////////////////////////////////////////////////////////

type TargetGatherState struct {
	support.TargetStateSupport[*db.OperatorState, *db.GatherTargetState]
}

var _ model.TargetState = (*TargetGatherState)(nil)

func NewTargetGatherState(n *OperatorState) *TargetGatherState {
	return &TargetGatherState{support.NewTargetStateSupport[*db.OperatorState, *db.GatherTargetState](n, mymetamodel.PHASE_GATHER)}
}

func (c *TargetGatherState) GetLinks() []ElementId {
	t := c.Get()
	if t == nil {
		return nil
	}

	return support.LinksForTypePhase(mymetamodel.TYPE_VALUE_STATE, c.GetNamespace(), mymetamodel.PHASE_PROPAGATE, maputils.OrderedValues(t.Spec.Operands)...)
}

func (c *TargetGatherState) GetOperations() map[string]db.Operation {
	return c.Get().Spec.Operations
}

func (c *TargetGatherState) GetOperands() map[string]string {
	return c.Get().Spec.Operands
}

func (c *TargetGatherState) GetOutputs() map[string]string {
	return c.Get().Spec.Outputs
}
