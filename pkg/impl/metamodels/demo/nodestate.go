package demo

import (
	"fmt"
	"reflect"

	. "github.com/mandelsoft/engine/pkg/processing/mmids"

	"github.com/mandelsoft/engine/pkg/processing/metamodel/objectbase"
	"github.com/mandelsoft/engine/pkg/processing/metamodel/objectbase/wrapped"
	"github.com/mandelsoft/engine/pkg/processing/mmids"
	"github.com/mandelsoft/engine/pkg/processing/model"
	"github.com/mandelsoft/engine/pkg/processing/model/support"

	"github.com/mandelsoft/engine/pkg/impl/metamodels/demo/db"
	mymetamodel "github.com/mandelsoft/engine/pkg/metamodels/demo"
)

func init() {
	wrapped.MustRegisterType[NodeState](scheme)
}

type NodeState struct {
	support.InternalObjectSupport
}

var _ model.InternalObject = (*NodeState)(nil)

func (n *NodeState) GetCurrentState(phase Phase) model.CurrentState {
	return &CurrentState{n}
}

func (n *NodeState) GetTargetState(phase Phase) model.TargetState {
	return &TargetState{n}
}

func (n *NodeState) SetExternalState(lcxt model.Logging, ob objectbase.Objectbase, phase Phase, state model.ExternalStates) error {
	_, err := wrapped.Modify(ob, n, func(_o support.DBObject) (bool, bool) {
		t := _o.(*db.NodeState).Target
		if t == nil {
			t = &db.TargetState{}
		}

		mod := false
		for _, _s := range state { // we have just one external object here, but just for demonstration
			s := _s.(*ExternalNodeState).GetState()
			m := !reflect.DeepEqual(t.Spec, *s) || t.ObjectVersion != _s.GetVersion()
			if m {
				t.Spec = *s
				t.ObjectVersion = _s.GetVersion()
			}
			mod = mod || m
		}

		_o.(*db.NodeState).Target = t
		return mod, mod
	})
	return err
}

func (n *NodeState) Process(req model.Request) model.ProcessingREsult {
	log := req.Logging.Logger(REALM)

	err := n.Validate()
	if err != nil {
		return model.ProcessingREsult{
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

	return model.StatusCompleted(NewOutputState(out))
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

func (n *NodeState) Commit(lctx model.Logging, ob objectbase.Objectbase, phase Phase, id RunId, commit *model.CommitInfo) (bool, error) {
	return n.InternalObjectSupport.Commit(lctx, ob, phase, id, commit, support.CommitFunc(n.commitTargetState))
}

func (n *NodeState) commitTargetState(lctx model.Logging, _o support.InternalDBObject, phase Phase, spec *model.CommitInfo) {
	o := _o.(*db.NodeState)
	log := lctx.Logger(REALM)
	if o.Target != nil && spec != nil {
		o.Current.Operands = o.Target.Spec.Operands
		o.Current.InputVersion = spec.InputVersion
		log.Info("Commit object version for NodeState {{name}}", "name", o.Name)
		log.Info("  object version {{version}}", "version", o.Target.ObjectVersion)
		o.Current.ObjectVersion = o.Target.ObjectVersion
		o.Current.OutputVersion = spec.State.(*OutputState).GetOutputVersion()
		o.Current.Output.Value = spec.State.(*OutputState).GetState()
	} else {
		log.Info("bothing to commit for NodeState {{name}}", "name", o.Name)
	}
	o.Target = nil
}

////////////////////////////////////////////////////////////////////////////////

type OutputState = support.OutputState[int]

var NewOutputState = support.NewOutputState[int]

////////////////////////////////////////////////////////////////////////////////

type CurrentState struct {
	n *NodeState
}

var _ model.CurrentState = (*CurrentState)(nil)

func (c *CurrentState) get() *db.NodeState {
	return c.n.GetBase().(*db.NodeState)
}

func (c *CurrentState) GetLinks() []ElementId {
	var r []ElementId

	for _, o := range c.get().Current.Operands {
		r = append(r, mmids.NewElementId(c.n.GetType(), c.n.GetNamespace(), o, mymetamodel.PHASE_UPDATING))
	}
	return r
}

func (c *CurrentState) GetInputVersion() string {
	return c.get().Current.InputVersion
}

func (c *CurrentState) GetObjectVersion() string {
	return c.get().Current.ObjectVersion
}

func (c *CurrentState) GetOutputVersion() string {
	return c.get().Current.OutputVersion
}

func (c *CurrentState) GetOutput() model.OutputState {
	return support.NewOutputState[int](c.get().Current.Output.Value)
}

////////////////////////////////////////////////////////////////////////////////

type TargetState struct {
	n *NodeState
}

var _ model.TargetState = (*TargetState)(nil)

func (c *TargetState) get() *db.NodeState {
	return c.n.GetBase().(*db.NodeState)
}

func (c *TargetState) GetLinks() []mmids.ElementId {
	var r []ElementId

	t := c.get().Target
	if t == nil {
		return nil
	}

	for _, o := range t.Spec.Operands {
		r = append(r, mmids.NewElementId(c.n.GetType(), c.n.GetNamespace(), o, mymetamodel.PHASE_UPDATING))
	}
	return r
}

func (c *TargetState) GetObjectVersion() string {
	return c.get().Target.ObjectVersion
}

func (c *TargetState) GetInputVersion(inputs model.Inputs) string {
	return support.DefaultInputVersion(inputs)
}

func (c *TargetState) GetOperator() *db.OperatorName {
	return c.get().Target.Spec.Operator
}

func (c *TargetState) GetValue() *int {
	return c.get().Target.Spec.Value
}
