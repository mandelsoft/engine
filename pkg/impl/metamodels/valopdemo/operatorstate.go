package valopdemo

import (
	"fmt"
	"reflect"

	"github.com/mandelsoft/engine/pkg/metamodel/common"
	"github.com/mandelsoft/engine/pkg/metamodel/model"
	"github.com/mandelsoft/engine/pkg/metamodel/model/support"
	"github.com/mandelsoft/engine/pkg/metamodel/objectbase"
	"github.com/mandelsoft/engine/pkg/metamodel/objectbase/wrapped"
	"github.com/mandelsoft/logging"

	"github.com/mandelsoft/engine/pkg/impl/metamodels/valopdemo/db"
	mymetamodel "github.com/mandelsoft/engine/pkg/metamodels/valopdemo"
)

func init() {
	wrapped.MustRegisterType[OperatorState](scheme)
}

type OperatorState struct {
	support.InternalObjectSupport
}

type NodeStateCurrent struct {
	Result int `json:"result"`
}

var _ model.InternalObject = (*OperatorState)(nil)

func (n *OperatorState) GetCurrentState(phase model.Phase) model.CurrentState {
	switch phase {
	case mymetamodel.PHASE_GATHER:
		return &CurrentGatherState{n}
	case mymetamodel.PHASE_CALCULATION:
		return &CurrentCalcState{n}
	}
	return nil
}

func (n *OperatorState) GetTargetState(phase model.Phase) model.TargetState {
	switch phase {
	case mymetamodel.PHASE_GATHER:
		return &TargetGatherState{n}
	case mymetamodel.PHASE_CALCULATION:
		return &TargetCalcState{n}
	}
	return nil
}

func (n *OperatorState) SetExternalState(lctx common.Logging, ob objectbase.Objectbase, phase model.Phase, state common.ExternalStates) error {
	log := lctx.Logger(REALM).WithValues("name", n.GetName(), "phase", phase)
	_, err := wrapped.Modify(ob, n, func(_o support.DBObject) (bool, bool) {
		o := _o.(*db.OperatorState)
		mod := false
		for _, s := range state {
			n.setExternalObjectState(log, o, s.(*ExternalOperatorState), &mod)
			switch phase {
			case mymetamodel.PHASE_GATHER:
				n.setExternalGatherState(log, o, &mod)
			case mymetamodel.PHASE_CALCULATION:
				n.setExternalCalcState(log, o, s.(*ExternalOperatorState), &mod)
			}
		}
		return mod, mod
	})
	return err
}

func (n *OperatorState) setExternalObjectState(log logging.Logger, o *db.OperatorState, state *ExternalOperatorState, mod *bool) {
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

func (n *OperatorState) setExternalGatherState(log logging.Logger, o *db.OperatorState, mod *bool) {
	t := o.Gather.Target
	if t == nil {
		t = &db.GatherTargetState{}
	}

	log.Info("set target state for phase {{phase}} of OperatorState {{name}}")
	support.UpdateField(&t.ObjectVersion, &o.Target.ObjectVersion, mod)
	o.Gather.Target = t
}

func (n *OperatorState) setExternalCalcState(log logging.Logger, o *db.OperatorState, state *ExternalOperatorState, mod *bool) {
	t := o.Calculation.Target
	if t == nil {
		t = &db.CalculationTargetState{}
	}

	log.Info("set target state for phase {{phase}} of NodeState {{name}}")
	support.UpdateField(&t.ObjectVersion, &o.Target.ObjectVersion, mod)
	o.Calculation.Target = t
}

func (n *OperatorState) Process(ob objectbase.Objectbase, req model.Request) model.Status {
	switch req.Element.GetPhase() {
	case mymetamodel.PHASE_GATHER:
		return n.processGather(ob, req)
	case mymetamodel.PHASE_CALCULATION:
		return n.processCalc(ob, req)
	}
	return model.Status{
		Status: common.STATUS_FAILED,
		Error:  fmt.Errorf("unknoen phase %q", req.Element.GetPhase()),
	}
}

func (n *OperatorState) processGather(ob objectbase.Objectbase, req model.Request) model.Status {
	log := req.Logging.Logger(REALM)

	err := n.Validate()
	if err != nil {
		return model.Status{
			Status: common.STATUS_FAILED, // final failure
			Error:  err,
		}
	}

	links := n.GetTargetState(req.Element.GetPhase()).GetLinks()
	operands := make([]db.Operand, len(links))
	for iid, e := range req.Inputs {
		s := e.(*CurrentState)
		for i, oid := range links {
			if iid == oid {
				operands[i] = db.Operand{
					Origin: iid.ObjectId(),
					Value:  s.GetOutput(),
				}
				log.Info("found operand {{index}} from {{link}}: {{value}}", "index", i, "link", iid, "value", operands[i].Value)
				break
			}
		}
	}

	if len(links) == 0 {
		operands = []db.Operand{
			{
				Origin: common.NewObjectIdFor(req.Element.GetObject()),
				Value:  0,
			},
		}
	}
	return model.Status{
		Status:      common.STATUS_COMPLETED,
		ResultState: NewGatherResultState(operands),
	}
}

func (n *OperatorState) processCalc(ob objectbase.Objectbase, req model.Request) model.Status {
	log := req.Logging.Logger(REALM)

	err := n.Validate()
	if err != nil {
		return model.Status{
			Status:      common.STATUS_FAILED, // final failure
			ResultState: nil,
			Error:       err,
		}
	}

	var operands []db.Operand
	for _, l := range req.Inputs {
		operands = l.(*CurrentGatherState).GetOutput()
	}
	s := n.GetTargetState(req.Element.GetPhase()).(*TargetCalcState)
	op := s.GetOperator()

	out := operands[0].Value
	log.Info("calculate {{operator}} {{operands}}", "operator", op, "operands", operands)
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
		ResultState: NewCalcResultState(out),
	}
}

func (n *OperatorState) Validate() error {
	_s := n.GetTargetState(mymetamodel.PHASE_GATHER)
	if _s == nil {
		return nil
	}
	s := _s.(*TargetGatherState)

	op := s.GetOperator()
	if op == "" {
		return fmt.Errorf("operator missing")
	}

	if len(s.GetLinks()) == 0 {
		return fmt.Errorf("operator node requires at least one operand")
	}
	switch op {
	case db.OP_ADD:
	case db.OP_SUB:
	case db.OP_DIV:
	case db.OP_MUL:
	default:
		return fmt.Errorf("unknown operator %q", op)
	}
	return nil
}

func (n *OperatorState) Commit(lctx common.Logging, ob objectbase.Objectbase, phase common.Phase, id model.RunId, commit *model.CommitInfo) (bool, error) {
	return n.InternalObjectSupport.Commit(lctx, ob, phase, id, commit, support.CommitFunc(n.commitTargetState))
}

func (n *OperatorState) commitTargetState(lctx common.Logging, _o support.InternalDBObject, phase model.Phase, spec *model.CommitInfo) {
	o := _o.(*db.OperatorState)
	log := lctx.Logger(REALM).WithValues("name", o.Name, "phase", phase)
	switch phase {
	case mymetamodel.PHASE_GATHER:
		if o.Gather.Target != nil && spec != nil {
			// update phase specific state
			log.Info("commit phase {{phase}} for OperatorState {{name}}")
			log.Info("  input version {{inpvers}}", "inpvers", spec.InputVersion)
			log.Info("  object version {{objvers}}", "objvers", o.Gather.Target.ObjectVersion)
			log.Info("  output version {{outvers}}", "outvers", spec.State.(*GatherResultState).GetOutputVersion())
			log.Info("  output {{output}}", "output", spec.State.(*GatherResultState).GetState())
			c := &o.Gather.Current
			c.InputVersion = spec.InputVersion
			c.ObjectVersion = o.Gather.Target.ObjectVersion
			c.OutputVersion = spec.State.(*GatherResultState).GetOutputVersion()
			c.Output.Values = spec.State.(*GatherResultState).GetState()
		}
		o.Gather.Target = nil

	case mymetamodel.PHASE_CALCULATION:
		if o.Calculation.Target != nil && spec != nil {
			// update state specific
			log.Info("commit phase {{phase}} for OperatorState {{name}}")
			log.Info("  input version {{inpvers}}", "inpvers", spec.InputVersion)
			log.Info("  object version {{objvers}}", "objvers", o.Calculation.Target.ObjectVersion)
			log.Info("  output version {{outvers}}", "outvers", spec.State.(*CalcResultState).GetOutputVersion())
			log.Info("  output {{output}}", "output", spec.State.(*CalcResultState).GetState())
			c := &o.Calculation.Current
			c.InputVersion = spec.InputVersion
			c.ObjectVersion = o.Calculation.Target.ObjectVersion
			c.OutputVersion = spec.State.(*CalcResultState).GetOutputVersion()
			c.Output.Value = spec.State.(*CalcResultState).GetState()

			// ... and common state for last phase
			log.Info("  operands {{operands}}", "operands", o.Target.Spec.Operands)
			o.Current.Operands = o.Target.Spec.Operands
		}
		o.Calculation.Target = nil
		o.Target = nil
	}
}

////////////////////////////////////////////////////////////////////////////////

type GatherResultState = support.ResultState[[]db.Operand]
type CalcResultState = support.ResultState[int]

var NewGatherResultState = support.NewResultState[[]db.Operand]
var NewCalcResultState = support.NewResultState[int]

////////////////////////////////////////////////////////////////////////////////

type CurrentGatherState struct {
	n *OperatorState
}

var _ model.CurrentState = (*CurrentGatherState)(nil)

func (c *CurrentGatherState) get() *db.GatherCurrentState {
	return &c.n.GetBase().(*db.OperatorState).Gather.Current
}

func (c *CurrentGatherState) GetLinks() []model.ElementId {
	var r []model.ElementId

	for _, o := range c.n.GetBase().(*db.OperatorState).Current.Operands {
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

func (c *CurrentGatherState) GetOutput() []db.Operand {
	return c.get().Output.Values
}

type CurrentCalcState struct {
	n *OperatorState
}

var _ model.CurrentState = (*CurrentCalcState)(nil)

func (c *CurrentCalcState) get() *db.CalculationCurrentState {
	return &c.n.GetBase().(*db.OperatorState).Calculation.Current
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

func (c *CurrentCalcState) GetOutput() int {
	return c.get().Output.Value
}

////////////////////////////////////////////////////////////////////////////////

type TargetGatherState struct {
	n *OperatorState
}

var _ model.TargetState = (*TargetGatherState)(nil)

func (c *TargetGatherState) get() *db.GatherTargetState {
	return c.n.GetBase().(*db.OperatorState).Gather.Target
}

func (c *TargetGatherState) GetLinks() []common.ElementId {
	var r []model.ElementId

	t := c.n.GetBase().(*db.OperatorState).Target
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

func (c *TargetGatherState) GetOperator() db.OperatorName {
	return c.n.GetBase().(*db.OperatorState).Target.Spec.Operator
}

type TargetCalcState struct {
	n *OperatorState
}

var _ model.TargetState = (*TargetCalcState)(nil)

func (c *TargetCalcState) get() *db.CalculationTargetState {
	return c.n.GetBase().(*db.OperatorState).Calculation.Target
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

func (c *TargetCalcState) GetOperator() db.OperatorName {
	return c.n.GetBase().(*db.OperatorState).Target.Spec.Operator
}
