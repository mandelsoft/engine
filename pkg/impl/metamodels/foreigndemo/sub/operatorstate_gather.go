package sub

import (
	"fmt"
	"strconv"

	. "github.com/mandelsoft/engine/pkg/processing/mmids"

	"github.com/mandelsoft/logging"

	"github.com/mandelsoft/engine/pkg/processing/model"
	"github.com/mandelsoft/engine/pkg/processing/model/support"
	"github.com/mandelsoft/engine/pkg/processing/objectbase"

	"github.com/mandelsoft/engine/pkg/impl/metamodels/foreigndemo/sub/db"
	mymetamodel "github.com/mandelsoft/engine/pkg/metamodels/foreigndemo"
)

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
		case db.OP_ADD:
		case db.OP_SUB:
		case db.OP_DIV:
		case db.OP_MUL:
		default:
			return model.ACCEPT_INVALID, fmt.Errorf("unknown operator %q for operation %q", op, n)
		}

		for i, c := range e.Operands {
			if _, err := strconv.Atoi(c); err != nil {
				if _, ok := s.Operands[c]; !ok {
					return model.ACCEPT_INVALID, fmt.Errorf("operand %d of operation %q: unknown input %q", i+1, n, c)
				}
			}
		}
	}
	return model.ACCEPT_OK, nil
}

func (g GatherPhase) GetCurrentState(o *OperatorState, phase Phase) model.CurrentState {
	return NewCurrentGatherState(o)
}

func (g GatherPhase) GetTargetState(o *OperatorState, phase Phase) model.TargetState {
	return NewTargetGatherState(o)
}

func (_ GatherPhase) DBSetExternalState(log logging.Logger, o *db.OperatorState, phase Phase, state model.ExternalState, mod *bool) {
	t := o.Gather.Target

	log.Info("set target state for phase {{phase}} of OperatorState {{name}}")
	support.UpdateField(&t.Spec, state.(*ExternalOperatorState).GetState(), mod)
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
	operands := map[string]db.Operand{}
	for iid, e := range req.Inputs {
		s := e.(*ValueOutputState).GetState()
		for n, src := range t.GetOperands() {
			if iid.GetName() == src {
				operands[n] = db.Operand{
					Origin: iid.ObjectId(),
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
				Origin: req.Element.Id().ObjectId(),
				Value:  v,
			}
			log.Info("found inline operand {{name}}: {{value}}", "name", n, "value", v)
		}
	}

	// check target expression object.
	err := req.SlaveManagement.AssureSlaves(
		nil,
		support.SlaveCreationOnly,
		model.SlaveId(req.Element.Id(), mymetamodel.TYPE_EXPRESSION_STATE, mymetamodel.PHASE_CALCULATING),
	)

	if err != nil {
		return model.StatusCompleted(nil, err)
	}

	out := &db.GatherOutput{
		Operands:   operands,
		Operations: t.GetOperations(),
	}
	return model.StatusCompleted(NewGatherOutputState(req.FormalVersion, out))
}

func (_ GatherPhase) PrepareDeletion(log logging.Logger, ob objectbase.Objectbase, o *OperatorState, phase Phase) error {
	oid := model.SlaveObjectId(o, mymetamodel.TYPE_EXPRESSION)
	return support.RequestSlaveDeletion(log, ob, oid)
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
	var r []ElementId

	t := c.Get()
	if t == nil {
		return nil
	}

	for _, o := range t.Spec.Operands {
		r = append(r, NewElementId(mymetamodel.TYPE_VALUE_STATE, c.GetNamespace(), o, mymetamodel.PHASE_PROPAGATE))
	}
	return r
}

func (c *TargetGatherState) GetOperations() map[string]db.Operation {
	return c.Get().Spec.Operations
}

func (c *TargetGatherState) GetOperands() map[string]string {
	return c.Get().Spec.Operands
}
