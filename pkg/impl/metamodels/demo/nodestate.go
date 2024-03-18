package demo

import (
	"fmt"
	"strings"

	. "github.com/mandelsoft/engine/pkg/processing/mmids"
	"github.com/mandelsoft/goutils/generics"

	"github.com/mandelsoft/engine/pkg/impl/metamodels/demo/db"
	mymetamodel "github.com/mandelsoft/engine/pkg/metamodels/demo"
	"github.com/mandelsoft/engine/pkg/processing/model"
	"github.com/mandelsoft/engine/pkg/processing/model/support"
	db2 "github.com/mandelsoft/engine/pkg/processing/model/support/db"
	"github.com/mandelsoft/engine/pkg/processing/objectbase"
	"github.com/mandelsoft/engine/pkg/processing/objectbase/wrapped"
	"github.com/mandelsoft/engine/pkg/runtime"
)

func init() {
	wrapped.MustRegisterType[NodeState](scheme)
}

type NodeState struct {
	support.InternalObjectSupport[*db.NodeState]
}

var (
	_ model.InternalObject      = (*NodeState)(nil)
	_ runtime.InitializedObject = (*NodeState)(nil)
)

func (n *NodeState) Initialize() error {
	return support.SetPhaseStateAccess(n, db.NodePhaseStateAccess)
}

func (n *NodeState) GetCurrentState(phase Phase) model.CurrentState {
	return NewCurrentState(n)
}

func (n *NodeState) GetTargetState(phase Phase) model.TargetState {
	return NewTargetState(n)
}

func (n *NodeState) assureTarget(o *db.NodeState) *db.TargetState {
	return o.State.CreateTarget().(*db.TargetState)
}

func (n *NodeState) AcceptExternalState(lctx model.Logging, ob objectbase.Objectbase, phase Phase, state model.ExternalState) (model.AcceptStatus, error) {
	_, err := wrapped.Modify(ob, n, func(_o db2.Object) (bool, bool) {
		t := n.assureTarget(_o.(*db.NodeState))

		mod := false
		s := state.(*ExternalNodeState).GetState()
		support.UpdateField(&t.Spec, s, &mod)
		support.UpdateField(&t.ObjectVersion, generics.Pointer(state.GetVersion()), &mod)
		return mod, mod
	})
	return 0, err
}

func (n *NodeState) Process(req model.Request) model.ProcessingResult {
	log := req.Logging.Logger(REALM)

	err := n.Validate()
	if err != nil {
		return model.ProcessingResult{
			Status:      model.STATUS_FAILED, // final failure
			ResultState: nil,
			Error:       err,
		}
	}

	links := n.GetTargetState(req.Element.GetPhase()).GetLinks()
	operands := make([]int, len(links))
	origin := make([]ObjectId, len(links))
	for iid, e := range req.Inputs {
		s := e.(*support.OutputState[int]).GetState()
		for i, oid := range links {
			if iid == oid {
				operands[i] = s
				origin[i] = iid.ObjectId()
				log.Info("found operand {{index}} from {{link}}: {{value}}", "index", i, "link", iid, "value", operands[i])
				break
			}
		}
	}
	s := n.GetTargetState(req.Element.GetPhase()).(*TargetState)
	op := s.GetOperator()

	out := 0
	if op != nil {
		out = operands[0]
		switch *op {
		case db.OP_ADD:
			for _, v := range operands[1:] {
				out += v
			}
		case db.OP_SUB:
			for _, v := range operands[1:] {
				out -= v
			}
		case db.OP_MUL:
			for _, v := range operands[1:] {
				out *= v
			}
		case db.OP_DIV:
			for i, v := range operands[1:] {
				if v == 0 {
					return model.StatusFailed(fmt.Errorf("division by zero for operand %d[%s]", i, origin[i+1]))
				}
				out /= v
			}
		}
	} else {
		out = *s.GetValue()
	}

	return model.StatusCompleted(NewOutputState(req.FormalVersion, out))
}

func (n *NodeState) Validate() error {
	_s := n.GetTargetState(mymetamodel.PHASE_UPDATING)
	if _s == nil {
		return nil
	}
	s := _s.(*TargetState)

	op := s.GetOperator()
	if op != nil && s.GetValue() != nil {
		return fmt.Errorf("only operator or value can be specified")
	}
	if op == nil && s.GetValue() == nil {
		return fmt.Errorf("operator or value must be specified")
	}

	if op != nil {
		if len(s.GetLinks()) == 0 {
			return fmt.Errorf("operator node requires at least one operand")
		}
		switch *op {
		case db.OP_ADD:
		case db.OP_SUB:
		case db.OP_DIV:
		case db.OP_MUL:
		default:
			return fmt.Errorf("unknown operator %q", *op)
		}
	} else {
		if len(s.GetLinks()) != 0 {
			return fmt.Errorf("operands only possible for operator node")
		}
	}
	return nil
}

func (n *NodeState) Rollback(lctx model.Logging, ob objectbase.Objectbase, phase Phase, id RunId, tgt model.TargetState, formal *string) (bool, error) {
	return n.InternalObjectSupport.HandleRollback(lctx, ob, phase, id, tgt, formal, support.RollbackFunc[*db.NodeState](n.rollbackTargetState))
}

func (n *NodeState) rollbackTargetState(lctx model.Logging, o *db.NodeState, phase Phase) {
	if o.State.Target != nil {
		lctx.Logger().Info("  object version {{version}}", "version", o.State.Target.ObjectVersion)
		o.State.Current.ObservedOperands = o.State.Target.Spec.Operands
	}
}

func (n *NodeState) Commit(lctx model.Logging, ob objectbase.Objectbase, phase Phase, id RunId, commit *model.CommitInfo) (bool, error) {
	return n.InternalObjectSupport.HandleCommit(lctx, ob, phase, id, commit, support.CommitFunc[*db.NodeState](n.commitTargetState))
}

func (n *NodeState) commitTargetState(lctx model.Logging, o *db.NodeState, phase Phase, spec *model.CommitInfo) {
	log := lctx.Logger(REALM)
	if o.State.Target != nil && spec != nil {
		log.Info("  operands {{operands}}", "operands", strings.Join(o.State.Target.Spec.Operands, ","))
		o.State.Current.Operands = o.State.Target.Spec.Operands
		o.State.Current.ObservedOperands = o.State.Target.Spec.Operands
		log.Info("  input version {{version}}", "version", spec.InputVersion)
		o.State.Current.InputVersion = spec.InputVersion
		log.Info("  object version {{version}}", "version", o.State.Target.ObjectVersion)
		o.State.Current.ObjectVersion = o.State.Target.ObjectVersion
		log.Info("  output version {{version}}", "version", spec.OutputState.(*OutputState).GetOutputVersion())
		o.State.Current.OutputVersion = spec.OutputState.(*OutputState).GetOutputVersion()
		log.Info("  output {{output}}", "output", spec.OutputState.(*OutputState).GetState())
		o.State.Current.Output.Value = spec.OutputState.(*OutputState).GetState()
	} else {
		log.Info("  nothing to commit for NodeState {{name}}", "name", o.GetName())
	}
}

////////////////////////////////////////////////////////////////////////////////

type OutputState = support.OutputState[int]

var NewOutputState = support.NewOutputState[int]

////////////////////////////////////////////////////////////////////////////////

type CurrentState struct {
	support.CurrentStateSupport[*db.NodeState, *db.CurrentState]
}

var _ model.CurrentState = (*CurrentState)(nil)

func NewCurrentState(n *NodeState) *CurrentState {
	return &CurrentState{
		support.NewCurrentStateSupport[*db.NodeState, *db.CurrentState](n, mymetamodel.PHASE_UPDATING),
	}
}

func (c *CurrentState) GetObservedState() model.ObservedState {
	if c.GetObjectVersion() == c.GetObservedVersion() {
		return c
	}
	return c.GetObservedStateForTypeAndPhase(mymetamodel.TYPE_NODE_STATE, mymetamodel.PHASE_UPDATING, c.Get().ObservedOperands...)
}

func (c *CurrentState) GetLinks() []ElementId {
	return c.GetObservedStateForTypeAndPhase(mymetamodel.TYPE_NODE_STATE, mymetamodel.PHASE_UPDATING, c.Get().Operands...).GetLinks()
}

func (c *CurrentState) GetOutput() model.OutputState {
	return support.NewOutputState[int](c.GetFormalVersion(), c.Get().Output.Value)
}

////////////////////////////////////////////////////////////////////////////////

type TargetState struct {
	support.TargetStateSupport[*db.NodeState, *db.TargetState]
}

var _ model.TargetState = (*TargetState)(nil)

func NewTargetState(n *NodeState) *TargetState {
	return &TargetState{support.NewTargetStateSupport[*db.NodeState, *db.TargetState](n, mymetamodel.PHASE_UPDATING)}
}

func (c *TargetState) GetLinks() []ElementId {
	t := c.Get()
	if t == nil {
		return nil
	}
	return support.LinksForTypePhase(c.GetType(), c.GetNamespace(), mymetamodel.PHASE_UPDATING, t.Spec.Operands...)
}

func (c *TargetState) GetOperator() *db.OperatorName {
	return c.Get().Spec.Operator
}

func (c *TargetState) GetValue() *int {
	return c.Get().Spec.Value
}
