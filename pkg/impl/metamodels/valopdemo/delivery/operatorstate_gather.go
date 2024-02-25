package delivery

import (
	"fmt"

	. "github.com/mandelsoft/engine/pkg/processing/mmids"

	"github.com/mandelsoft/logging"

	"github.com/mandelsoft/engine/pkg/processing/model"
	"github.com/mandelsoft/engine/pkg/processing/model/support"

	"github.com/mandelsoft/engine/pkg/impl/metamodels/valopdemo/delivery/db"
	mymetamodel "github.com/mandelsoft/engine/pkg/metamodels/valopdemo"
)

type GatherPhase struct{ PhaseBase }

var _ OperatorStatePhase = (*GatherPhase)(nil)

func (_ GatherPhase) AcceptExternalState(log logging.Logger, o *OperatorState, state model.ExternalState, phase Phase) (model.AcceptStatus, error) {
	s := state.(*ExternalOperatorState).GetState()

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
	return model.ACCEPT_OK, nil
}

func (_ GatherPhase) GetCurrentState(o *OperatorState, phase Phase) model.CurrentState {
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

func (g GatherPhase) DBCommit(log logging.Logger, o *db.OperatorState, phase Phase, spec *model.CommitInfo, mod *bool) {
	if o.Gather.Target != nil && spec != nil {
		// update phase specific state
		log.Info("  output {{output}}", "output", spec.OutputState.(*GatherOutputState).GetState())
		c := &o.Gather.Current
		c.Output = *spec.OutputState.(*GatherOutputState).GetState()
	} else {
		log.Info("nothing to commit for phase {{phase}} of OperatorState {{name}}")
	}
}

func (g GatherPhase) Process(o *OperatorState, phase Phase, req model.Request) model.ProcessingResult {
	log := req.Logging.Logger()
	t := g.getTargetState(o)

	links := t.GetLinks()
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
				Origin: NewObjectIdFor(req.Element.GetObject()),
				Value:  0,
			},
		}
	}
	return model.StatusCompleted(NewGatherOutputState(req.FormalVersion, &db.GatherOutput{
		Operations: t.GetOperations(),
		Operands:   operands,
	}))
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

	for _, o := range c.Get().Operands {
		r = append(r, NewElementId(mymetamodel.TYPE_VALUE_STATE, c.GetNamespace(), o, mymetamodel.PHASE_PROPAGATE))
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

func (c *TargetGatherState) GetOperations() []db.Operation {
	return c.Get().Spec.Operations
}
