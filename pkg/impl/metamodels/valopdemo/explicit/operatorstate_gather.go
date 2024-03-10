package explicit

import (
	"fmt"
	"strings"

	. "github.com/mandelsoft/engine/pkg/processing/mmids"
	db2 "github.com/mandelsoft/engine/pkg/processing/model/support/db"

	"github.com/mandelsoft/logging"

	"github.com/mandelsoft/engine/pkg/processing/model"
	"github.com/mandelsoft/engine/pkg/processing/model/support"

	"github.com/mandelsoft/engine/pkg/impl/metamodels/valopdemo/explicit/db"
	mymetamodel "github.com/mandelsoft/engine/pkg/metamodels/valopdemo"
)

////////////////////////////////////////////////////////////////////////////////
// Gather Phase
////////////////////////////////////////////////////////////////////////////////

type GatherPhase struct{ PhaseBase }

var _ OperatorStatePhase = (*GatherPhase)(nil)

func (_ GatherPhase) AcceptExternalState(log logging.Logger, o *OperatorState, state model.ExternalState, phase Phase) (model.AcceptStatus, error) {
	s := state.(*ExternalOperatorState).GetState()

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

func (_ GatherPhase) DBRollback(log logging.Logger, o *db.OperatorState, phase Phase, mod *bool) {
	if o.Gather.Target != nil {
		c := &o.Gather.Current
		log.Info("  observed operands {{operands}}", "operands", strings.Join(o.Gather.Target.Spec.Operands, ","))
		c.ObservedOperands = o.Gather.Target.Spec.Operands
	}
}

func (_ GatherPhase) DBCommit(log logging.Logger, o *db.OperatorState, phase Phase, spec *model.CommitInfo, mod *bool) {
	if spec != nil {
		c := &o.Gather.Current
		log.Info("  operands {{operands}}", "operands", o.Gather.Target.Spec.Operands)
		c.Operands = o.Gather.Target.Spec.Operands
		c.ObservedOperands = o.Gather.Target.Spec.Operands
		log.Info("  output {{output}}", "output", spec.OutputState.(*GatherOutputState).GetState())
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
					Origin: db2.NewObjectIdFor(iid),
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
				Origin: db2.NewObjectIdFor(req.Element),
				Value:  0,
			},
		}
	}
	return model.StatusCompleted(NewGatherOutputState(req.FormalVersion, &db.GatherOutput{
		Operator: t.GetOperator(),
		Operands: operands,
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

func (c *CurrentGatherState) GetObservedState() model.ObservedState {
	if c.GetObjectVersion() == c.GetObservedVersion() {
		return c
	}
	return c.GetObservedStateForTypeAndPhase(mymetamodel.TYPE_VALUE_STATE, mymetamodel.PHASE_PROPAGATE, c.Get().ObservedOperands...)
}

func (c *CurrentGatherState) GetLinks() []ElementId {
	return c.GetObservedStateForTypeAndPhase(mymetamodel.TYPE_VALUE_STATE, mymetamodel.PHASE_PROPAGATE, c.Get().Operands...).GetLinks()
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
	return support.LinksForTypePhase(mymetamodel.TYPE_VALUE_STATE, c.GetNamespace(), mymetamodel.PHASE_PROPAGATE, t.Spec.Operands...)
}

func (c *TargetGatherState) GetOperands() []string {
	return c.Get().Spec.Operands
}

func (c *TargetGatherState) GetOperator() db.OperatorName {
	return c.Get().Spec.Operator
}
