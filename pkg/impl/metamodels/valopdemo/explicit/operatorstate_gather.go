package explicit

import (
	"fmt"

	. "github.com/mandelsoft/engine/pkg/processing/mmids"

	"github.com/mandelsoft/logging"

	"github.com/mandelsoft/engine/pkg/processing/mmids"
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

func (_ GatherPhase) AcceptExternalState(log logging.Logger, o *OperatorState, state model.ExternalState, phase mmids.Phase) (model.AcceptStatus, error) {

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

func (_ GatherPhase) DBCommit(log logging.Logger, o *db.OperatorState, phase Phase, spec *model.CommitInfo, mod *bool) {
	if spec != nil {
		c := &o.Gather.Current
		log.Info("  operands {{operands}}", "operands", o.Gather.Target.Spec.Operands)
		c.Operands = o.Gather.Target.Spec.Operands
		log.Info("  output {{output}}", "output", spec.OutputState.(*GatherOutputState).GetState())
		c.Output = *spec.OutputState.(*GatherOutputState).GetState()
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
				Origin: mmids.NewObjectIdFor(req.Element.GetObject()),
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

func (c *CurrentGatherState) GetLinks() []ElementId {
	var r []ElementId

	for _, o := range c.Get().Operands {
		r = append(r, mmids.NewElementId(mymetamodel.TYPE_VALUE_STATE, c.GetNamespace(), o, mymetamodel.PHASE_PROPAGATE))
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
