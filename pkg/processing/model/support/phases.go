package support

import (
	"fmt"

	"github.com/mandelsoft/engine/pkg/processing/internal"
	"github.com/mandelsoft/engine/pkg/processing/mmids"
	"github.com/mandelsoft/engine/pkg/processing/model"
	"github.com/mandelsoft/engine/pkg/processing/model/support/db"
	"github.com/mandelsoft/engine/pkg/processing/objectbase"
	"github.com/mandelsoft/engine/pkg/processing/objectbase/wrapped"
	"github.com/mandelsoft/engine/pkg/runtime"
	"github.com/mandelsoft/engine/pkg/utils"
	"github.com/mandelsoft/logging"
)

type _ = runtime.InitializedObject // use runtime package for go doc

type Phase[I InternalObject, T db.InternalDBObject] interface {
	DBCommit(log logging.Logger, o T, phase mmids.Phase, spec *model.CommitInfo, mod *bool)
	DBSetExternalFormalObjectVersion(log logging.Logger, t db.TargetState, phase mmids.Phase, state model.ExternalState, mod *bool)
	DBSetExternalState(log logging.Logger, o T, phase mmids.Phase, state model.ExternalState, mod *bool)
	DBRollback(log logging.Logger, o T, phase mmids.Phase, mod *bool)

	AcceptExternalState(log logging.Logger, o I, state model.ExternalState, phase mmids.Phase) (model.AcceptStatus, error)
	GetExternalState(o I, ext model.ExternalObject, phase mmids.Phase) model.ExternalState
	GetCurrentState(o I, phase mmids.Phase) model.CurrentState
	GetTargetState(o I, phase mmids.Phase) model.TargetState

	Process(o I, phase mmids.Phase, req model.Request) model.ProcessingResult
	PrepareDeletion(log logging.Logger, mgmt model.SlaveManagement, o I, phase mmids.Phase) error
}

type Phases[I InternalObject, T db.InternalDBObject] interface {
	Register(name mmids.Phase, ph Phase[I, T])

	DBSetExternalState(lctx model.Logging, i InternalObject, _o db.InternalDBObject, phase mmids.Phase, s model.ExternalState, mod *bool)
	DBCommit(lctx model.Logging, _o db.InternalDBObject, phase mmids.Phase, commit *model.CommitInfo, mod *bool)
	DBRollback(ctx model.Logging, _o db.InternalDBObject, phase mmids.Phase, mod *bool)

	AcceptExternalState(lctx model.Logging, o InternalObject, phase mmids.Phase, states model.ExternalState) (model.AcceptStatus, error)
	GetExternalState(o InternalObject, ext model.ExternalObject, phase mmids.Phase) model.ExternalState
	GetCurrentState(o InternalObject, phase mmids.Phase) model.CurrentState
	GetTargetState(o InternalObject, phase mmids.Phase) model.TargetState

	PrepareDeletion(lctx model.Logging, mgmt model.SlaveManagement, o InternalObject, phase mmids.Phase) error
	Process(o InternalObject, req model.Request) model.ProcessingResult
}

type DefaultPhase[I InternalObject, T db.InternalDBObject] struct{}

func (_ DefaultPhase[I, T]) AcceptExternalState(log logging.Logger, o I, state model.ExternalState, phase mmids.Phase) (model.AcceptStatus, error) {
	return model.ACCEPT_OK, nil
}

func (_ DefaultPhase[I, T]) GetExternalState(o I, ext model.ExternalObject, phase mmids.Phase) model.ExternalState {
	return ext.GetState()
}

func (_ DefaultPhase[I, T]) PrepareDeletion(log logging.Logger, mgmt model.SlaveManagement, o I, phase mmids.Phase) error {
	return nil
}

func (_ DefaultPhase[I, T]) DBSetExternalFormalObjectVersion(log logging.Logger, t db.TargetState, phase mmids.Phase, state model.ExternalState, mod *bool) {
	v := ""
	if state != nil {
		v = state.GetVersion()
	}
	if t.SetFormalObjectVersion(v) {
		log.Info("  setting formal object version for phase {{phase}} to {{formal}}", "formal", v)
		*mod = true
	}
}

////////////////////////////////////////////////////////////////////////////////

type phases[I InternalObject, T db.InternalDBObject] struct {
	realm  logging.Realm
	phases map[mmids.Phase]Phase[I, T]
}

func NewPhases[I InternalObject, T db.InternalDBObject](realm logging.Realm) Phases[I, T] {
	return &phases[I, T]{
		realm,
		map[mmids.Phase]Phase[I, T]{},
	}
}

func (p *phases[I, T]) Register(name mmids.Phase, ph Phase[I, T]) {
	p.phases[name] = ph
}

func (p *phases[I, T]) AcceptExternalState(lctx model.Logging, o InternalObject, phase mmids.Phase, state model.ExternalState) (model.AcceptStatus, error) {
	ph := p.phases[phase]
	if ph != nil {
		log := lctx.Logger(p.realm).WithValues("name", o.GetName(), "phase", phase)
		return ph.AcceptExternalState(log, o.(I), state, phase)
	}
	return model.ACCEPT_INVALID, fmt.Errorf("unknown phase %q", phase)
}

func (p *phases[I, T]) GetExternalState(o InternalObject, ext model.ExternalObject, phase mmids.Phase) model.ExternalState {
	ph := p.phases[phase]
	if ph != nil {
		return ph.GetExternalState(o.(I), ext, phase)
	}
	return nil
}

func (p *phases[I, T]) GetCurrentState(o InternalObject, phase mmids.Phase) model.CurrentState {
	ph := p.phases[phase]
	if ph != nil {
		return ph.GetCurrentState(o.(I), phase)
	}
	return nil
}

func (p *phases[I, T]) GetTargetState(o InternalObject, phase mmids.Phase) model.TargetState {
	ph := p.phases[phase]
	if ph != nil {
		return ph.GetTargetState(o.(I), phase)
	}
	return nil
}

func (p *phases[I, T]) DBCommit(lctx model.Logging, _o db.InternalDBObject, phase mmids.Phase, commit *model.CommitInfo, mod *bool) {
	ph := p.phases[phase]
	if ph != nil {
		log := lctx.Logger(p.realm).WithValues("name", _o.GetName(), "phase", phase)
		ph.DBCommit(log, _o.(T), phase, commit, mod)
	}
}

func (p *phases[I, T]) DBSetExternalState(lctx model.Logging, i InternalObject, _o db.InternalDBObject, phase mmids.Phase, s model.ExternalState, mod *bool) {
	ph := p.phases[phase]
	if ph != nil {
		log := lctx.Logger(p.realm).WithValues("name", _o.GetName(), "phase", phase)
		t := i.GetPhaseStateFor(_o, phase).CreateTarget()
		if s != nil {
			t.SetObjectVersion(s.GetVersion())
		}
		ph.DBSetExternalFormalObjectVersion(log, t, phase, s, mod) // separated to provide a default implementation
		ph.DBSetExternalState(log, _o.(T), phase, s, mod)
	}
}

func (p *phases[I, T]) DBRollback(lctx model.Logging, _o db.InternalDBObject, phase mmids.Phase, mod *bool) {
	ph := p.phases[phase]
	if ph != nil {
		log := lctx.Logger(p.realm).WithValues("name", _o.GetName(), "phase", phase)
		ph.DBRollback(log, _o.(T), phase, mod)
	}
}

func (p *phases[I, T]) Process(o InternalObject, req model.Request) model.ProcessingResult {
	phase := req.Element.GetPhase()
	ph := p.phases[phase]
	if ph != nil {
		req.Logging = req.Logging.WithContext(p.realm)
		return ph.Process(o.(I), phase, req)
	}
	return model.ProcessingResult{
		Status: model.STATUS_FAILED,
		Error:  fmt.Errorf("unknown phase %q", phase),
	}
}

func (p *phases[I, T]) PrepareDeletion(lctx model.Logging, mgmt model.SlaveManagement, o InternalObject, phase mmids.Phase) error {
	ph := p.phases[phase]
	if ph != nil {
		log := lctx.Logger(p.realm).WithValues("name", o.GetName(), "phase", phase)
		return ph.PrepareDeletion(log, mgmt, o.(I), phase)
	}
	return nil
}

////////////////////////////////////////////////////////////////////////////////

// InternalPhaseObjectSupport provides complete support for
// [model.InternalObject] by using [Phases].
// It requires the effective [model.InternalObject] to implement
// [runtime.InitializedObject] to init the Phases attribute
type InternalPhaseObjectSupport[I InternalObject, T db.InternalDBObject] struct {
	InternalObjectSupport[T] `json:",inline"`
	self                     I
	phases                   Phases[I, T] `json:",omitempty"`
}

var _ model.InternalObject = (*InternalPhaseObjectSupport[InternalObject, db.InternalDBObject])(nil)

type selfer[I InternalObject, T db.InternalDBObject] interface {
	setSelf(i I, phases Phases[I, T], pi PhaseStateAccess[T])
}

func SetSelf[I InternalObject, T db.InternalDBObject](i I, phases Phases[I, T], phaseInfos PhaseStateAccess[T]) error {
	o, ok := utils.TryCast[selfer[I, T]](i)
	if !ok {
		return fmt.Errorf("invalid object type %T", i)
	}
	o.setSelf(i, phases, phaseInfos)
	return nil
}

func (n *InternalPhaseObjectSupport[I, T]) setSelf(i I, phases Phases[I, T], pi PhaseStateAccess[T]) {
	n.phases = phases
	n.self = i
	n.phaseInfos = pi
}

func (n *InternalPhaseObjectSupport[I, T]) GetDBObject() T {
	return utils.Cast[T](n.GetBase())
}

func (n *InternalPhaseObjectSupport[I, T]) GetExternalState(ext model.ExternalObject, phase mmids.Phase) model.ExternalState {
	return n.phases.GetExternalState(n.self, ext, phase)
}

func (n *InternalPhaseObjectSupport[I, T]) GetCurrentState(phase mmids.Phase) model.CurrentState {
	return n.phases.GetCurrentState(n.self, phase)
}

func (n *InternalPhaseObjectSupport[I, T]) GetTargetState(phase mmids.Phase) model.TargetState {
	return n.phases.GetTargetState(n.self, phase)
}

func (n *InternalPhaseObjectSupport[I, T]) AcceptExternalState(lctx model.Logging, ob objectbase.Objectbase, phase mmids.Phase, state model.ExternalState) (model.AcceptStatus, error) {
	n.Lock.Lock()
	defer n.Lock.Unlock()

	status, err := n.phases.AcceptExternalState(lctx, n.self, phase, state)
	if err != nil {
		return status, err
	}
	mod := func(_o db.Object) (bool, bool) {
		mod := false
		n.phases.DBSetExternalState(lctx, n, _o.(db.InternalDBObject), phase, state, &mod)
		return mod, mod
	}
	_, err = wrapped.Modify(ob, n, mod)
	return status, err
}

func (n *InternalPhaseObjectSupport[I, T]) PrepareDeletion(lctx internal.Logging, mgmt internal.SlaveManagement, phase mmids.Phase) error {
	return n.phases.PrepareDeletion(lctx, mgmt, n.self, phase)
}

func (n *InternalPhaseObjectSupport[I, T]) Process(request model.Request) model.ProcessingResult {
	return n.phases.Process(n.self, request)
}

func (n *InternalPhaseObjectSupport[I, T]) Rollback(lctx model.Logging, ob objectbase.Objectbase, phase mmids.Phase, id mmids.RunId, tgt model.TargetState, formal *string) (bool, error) {
	n.Lock.Lock()
	defer n.Lock.Unlock()
	log := lctx.Logger()

	mod := func(_o db.Object) (bool, bool) {
		o := _o.(T)
		p := n.GetPhaseStateFor(o, phase)
		b := p.ClearLock(id)
		if b {
			log.Info("  runlock {{runid}} cleared", "runid", id)
			if formal != nil {
				log.Info("setting formal version {{formal}}", "formal", *formal)
				p.GetCurrent().SetFormalVersion(*formal)
			}
			if tgt != nil {
				p.GetCurrent().SetObservedVersion(p.GetTarget().GetObjectVersion())
			}
			n.phases.DBRollback(lctx, o, phase, &b)
			p.ClearTarget()
		} else {
			log.Error("{{element}} not locked for {{runid}} (found {{busy}})", "runid", id, "busy", p.GetLock())
		}
		return b, b
	}
	return wrapped.Modify(ob, n, mod)
}

func (n *InternalPhaseObjectSupport[I, T]) Commit(lctx model.Logging, ob objectbase.Objectbase, phase mmids.Phase, id mmids.RunId, commit *model.CommitInfo) (bool, error) {
	f := CommitFunc[T](func(lctx model.Logging, o T, phase mmids.Phase, spec *model.CommitInfo) {
		var b bool
		n.phases.DBCommit(lctx, o, phase, commit, &b)
	})
	return n.InternalObjectSupport.HandleCommit(lctx, ob, phase, id, commit, f)
}
