package multidemo

import (
	"fmt"
	"strings"

	. "github.com/mandelsoft/engine/pkg/processing/mmids"
	db2 "github.com/mandelsoft/engine/pkg/processing/model/support/db"

	"github.com/mandelsoft/logging"

	"github.com/mandelsoft/engine/pkg/processing/model"
	"github.com/mandelsoft/engine/pkg/processing/model/support"

	"github.com/mandelsoft/engine/pkg/impl/metamodels/multidemo/db"
	mymetamodel "github.com/mandelsoft/engine/pkg/metamodels/multidemo"
)

////////////////////////////////////////////////////////////////////////////////
// Gather Phase
////////////////////////////////////////////////////////////////////////////////

type GatherPhase struct{ PhaseBase }

var _ NodeStatePhase = (*GatherPhase)(nil)

func (_ GatherPhase) AcceptExternalState(log logging.Logger, o *NodeState, state model.ExternalState, phase Phase) (model.AcceptStatus, error) {
	s := state.(*ExternalNodeState).GetState()

	op := s.Operator
	if op != nil && s.Value != nil {
		return model.ACCEPT_INVALID, fmt.Errorf("only operator or value can be specified")
	}
	if op == nil && s.Value == nil {
		return model.ACCEPT_INVALID, fmt.Errorf("operator or value must be specified")
	}

	if op != nil {
		if len(s.Operands) == 0 {
			return model.ACCEPT_INVALID, fmt.Errorf("operator node requires at least one operand")
		}
		switch *op {
		case db.OP_ADD:
		case db.OP_SUB:
		case db.OP_DIV:
		case db.OP_MUL:
		default:
			return model.ACCEPT_INVALID, fmt.Errorf("unknown operator %q", *op)
		}
	} else {
		if len(s.Operands) != 0 {
			return model.ACCEPT_INVALID, fmt.Errorf("operands only possible for operator node")
		}
	}
	return model.ACCEPT_OK, nil
}

func (_ GatherPhase) GetCurrentState(o *NodeState, phase Phase) model.CurrentState {
	return NewCurrentGatherState(o)
}

func (g GatherPhase) GetTargetState(o *NodeState, phase Phase) model.TargetState {
	return g.getTargetState(o)
}

func (_ GatherPhase) getTargetState(o *NodeState) *TargetGatherState {
	return NewTargetGatherState(o)
}

func (_ GatherPhase) DBSetExternalState(log logging.Logger, o *db.NodeState, phase Phase, state model.ExternalState, mod *bool) {
	t := o.Gather.Target
	log.Info("set target state for phase {{phase}} of NodeState {{name}}")
	support.UpdateField(&t.Spec, state.(*ExternalNodeState).GetState(), mod)
}

func (_ GatherPhase) DBRollback(log logging.Logger, o *db.NodeState, phase Phase, mod *bool) {
	if o.Gather.Target != nil {
		c := &o.Gather.Current
		log.Info("  observed operands {{operands}}", "operands", strings.Join(o.Gather.Target.Spec.Operands, ","))
		c.ObservedOperands = o.Gather.Target.Spec.Operands
	}
}

func (_ GatherPhase) DBCommit(log logging.Logger, o *db.NodeState, phase Phase, spec *model.CommitInfo, mod *bool) {
	if spec != nil {
		c := &o.Gather.Current
		// update phase specific state
		log.Info("  operands {{operands}}", "operands", o.Gather.Target.Spec.Operands)
		c.Operands = o.Gather.Target.Spec.Operands
		c.ObservedOperands = o.Gather.Target.Spec.Operands
		log.Info("  output {{output}}", "output", spec.OutputState.(*GatherOutputState).GetState())
		c.Output = *spec.OutputState.(*GatherOutputState).GetState()
	}
}

func (p GatherPhase) Process(o *NodeState, phase Phase, req model.Request) model.ProcessingResult {
	log := req.Logging.Logger(REALM)
	t := p.getTargetState(o)

	links := t.GetLinks()
	operands := make([]db.Operand, len(links))
	for iid, e := range req.Inputs {
		s := e.(*CalcOutputState).GetState()
		for i, oid := range links {
			if iid == oid {
				operands[i] = db.Operand{
					Origin: db2.NewObjectIdFor(iid),
					Value:  s,
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
				Value:  *(NewTargetGatherState(o)).GetValue(),
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
	support.CurrentStateSupport[*db.NodeState, *db.GatherCurrentState]
}

func NewCurrentGatherState(n *NodeState) model.CurrentState {
	return &CurrentGatherState{support.NewCurrentStateSupport[*db.NodeState, *db.GatherCurrentState](n, mymetamodel.PHASE_GATHER)}
}

func (c *CurrentGatherState) GetObservedState() model.ObservedState {
	if c.GetObjectVersion() == c.GetObservedVersion() {
		return c
	}
	return c.GetObservedStateForTypeAndPhase(mymetamodel.TYPE_NODE_STATE, mymetamodel.PHASE_CALCULATION, c.Get().ObservedOperands...)
}

func (c *CurrentGatherState) GetLinks() []ElementId {
	return c.GetObservedStateForTypeAndPhase(mymetamodel.TYPE_NODE_STATE, mymetamodel.PHASE_CALCULATION, c.Get().Operands...).GetLinks()
}

func (c *CurrentGatherState) GetOutput() model.OutputState {
	return NewGatherOutputState(c.GetFormalVersion(), &c.Get().Output)
}

////////////////////////////////////////////////////////////////////////////////

type TargetGatherState struct {
	support.TargetStateSupport[*db.NodeState, *db.GatherTargetState]
}

var _ model.TargetState = (*TargetGatherState)(nil)

func NewTargetGatherState(n *NodeState) *TargetGatherState {
	return &TargetGatherState{support.NewTargetStateSupport[*db.NodeState, *db.GatherTargetState](n, mymetamodel.PHASE_GATHER)}
}

func (c *TargetGatherState) GetLinks() []ElementId {
	t := c.Get()
	if t == nil {
		return nil
	}
	return support.LinksForTypePhase(c.GetType(), c.GetNamespace(), mymetamodel.PHASE_CALCULATION, t.Spec.Operands...)
}

func (c *TargetGatherState) GetOperator() *db.OperatorName {
	return c.Get().Spec.Operator
}

func (c *TargetGatherState) GetValue() *int {
	return c.Get().Spec.Value
}
