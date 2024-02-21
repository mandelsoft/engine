package support

import (
	"fmt"

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

type Phase[I InternalObject, T db.InternalDBObject, E model.ExternalState] interface {
	DBCommit(log logging.Logger, o T, phase mmids.Phase, spec *model.CommitInfo, mod *bool)
	DBSetExternalState(log logging.Logger, o T, phase mmids.Phase, state E, mod *bool)
	DBRollback(log logging.Logger, o T, phase mmids.Phase, mod *bool)

	AcceptExternalState(log logging.Logger, o I, states model.ExternalStates, phase mmids.Phase) (model.AcceptStatus, error)
	GetExternalState(o I, ext model.ExternalObject, phase mmids.Phase) model.ExternalState
	GetCurrentState(o I, phase mmids.Phase) model.CurrentState
	GetTargetState(o I, phase mmids.Phase) model.TargetState

	Process(o I, phase mmids.Phase, req model.Request) model.ProcessingResult
	PrepareDeletion(log logging.Logger, ob objectbase.Objectbase, o I, phase mmids.Phase) error
}

type Phases[I InternalObject, T db.InternalDBObject, E model.ExternalState] interface {
	Register(name mmids.Phase, ph Phase[I, T, E])

	DBSetExternalState(lctx model.Logging, i InternalObject, _o db.InternalDBObject, phase mmids.Phase, s model.ExternalState, mod *bool)
	DBCommit(lctx model.Logging, _o db.InternalDBObject, phase mmids.Phase, commit *model.CommitInfo, mod *bool)
	DBRollback(ctx model.Logging, _o db.InternalDBObject, phase mmids.Phase, mod *bool)

	AcceptExternalState(lctx model.Logging, o InternalObject, phase mmids.Phase, states model.ExternalStates) (model.AcceptStatus, error)
	GetExternalState(o InternalObject, ext model.ExternalObject, phase mmids.Phase) model.ExternalState
	GetCurrentState(o InternalObject, phase mmids.Phase) model.CurrentState
	GetTargetState(o InternalObject, phase mmids.Phase) model.TargetState

	PrepareDeletion(lctx model.Logging, ob objectbase.Objectbase, o InternalObject, phase mmids.Phase) error
	Process(o InternalObject, req model.Request) model.ProcessingResult
}

type DefaultPhase[I InternalObject, T db.InternalDBObject] struct{}

func (_ DefaultPhase[I, T]) AcceptExternalState(log logging.Logger, o I, state model.ExternalStates, phase mmids.Phase) (model.AcceptStatus, error) {
	return model.ACCEPT_OK, nil
}

func (_ DefaultPhase[I, T]) GetExternalState(o I, ext model.ExternalObject, phase mmids.Phase) model.ExternalState {
	return ext.GetState()
}

func (_ DefaultPhase[I, T]) DBRollback(log logging.Logger, o T, phase mmids.Phase, mod *bool) {
}

func (_ DefaultPhase[I, T]) PrepareDeletion(log logging.Logger, ob objectbase.Objectbase, o I, phase mmids.Phase) error {
	return nil
}

////////////////////////////////////////////////////////////////////////////////

type phases[I InternalObject, T db.InternalDBObject, E model.ExternalState] struct {
	realm  logging.Realm
	phases map[mmids.Phase]Phase[I, T, E]
}

func NewPhases[I InternalObject, T db.InternalDBObject, E model.ExternalState](realm logging.Realm) Phases[I, T, E] {
	return &phases[I, T, E]{
		realm,
		map[mmids.Phase]Phase[I, T, E]{},
	}
}

func (p *phases[I, T, E]) Register(name mmids.Phase, ph Phase[I, T, E]) {
	p.phases[name] = ph
}

func (p *phases[I, T, E]) AcceptExternalState(lctx model.Logging, o InternalObject, phase mmids.Phase, states model.ExternalStates) (model.AcceptStatus, error) {
	ph := p.phases[phase]
	if ph != nil {
		log := lctx.Logger(p.realm).WithValues("name", o.GetName(), "phase", phase)
		return ph.AcceptExternalState(log, o.(I), states, phase)
	}
	return model.ACCEPT_INVALID, fmt.Errorf("unknown phase %q", phase)
}

func (p *phases[I, T, E]) GetExternalState(o InternalObject, ext model.ExternalObject, phase mmids.Phase) model.ExternalState {
	ph := p.phases[phase]
	if ph != nil {
		return ph.GetExternalState(o.(I), ext, phase)
	}
	return nil
}

func (p *phases[I, T, E]) GetCurrentState(o InternalObject, phase mmids.Phase) model.CurrentState {
	ph := p.phases[phase]
	if ph != nil {
		return ph.GetCurrentState(o.(I), phase)
	}
	return nil
}

func (p *phases[I, T, E]) GetTargetState(o InternalObject, phase mmids.Phase) model.TargetState {
	ph := p.phases[phase]
	if ph != nil {
		return ph.GetTargetState(o.(I), phase)
	}
	return nil
}

func (p *phases[I, T, E]) DBCommit(lctx model.Logging, _o db.InternalDBObject, phase mmids.Phase, commit *model.CommitInfo, mod *bool) {
	ph := p.phases[phase]
	if ph != nil {
		log := lctx.Logger(p.realm).WithValues("name", _o.GetName(), "phase", phase)
		ph.DBCommit(log, _o.(T), phase, commit, mod)
	}
}

func (p *phases[I, T, E]) DBSetExternalState(lctx model.Logging, i InternalObject, _o db.InternalDBObject, phase mmids.Phase, s model.ExternalState, mod *bool) {
	ph := p.phases[phase]
	if ph != nil {
		log := lctx.Logger(p.realm).WithValues("name", _o.GetName(), "phase", phase)
		i.GetPhaseStateFor(_o, phase).CreateTarget().SetObjectVersion(s.GetVersion()) // TODO: handle multiple states
		ph.DBSetExternalState(log, _o.(T), phase, s.(E), mod)
	}
}

func (p *phases[I, T, E]) DBRollback(lctx model.Logging, _o db.InternalDBObject, phase mmids.Phase, mod *bool) {
	ph := p.phases[phase]
	if ph != nil {
		log := lctx.Logger(p.realm).WithValues("name", _o.GetName(), "phase", phase)
		ph.DBRollback(log, _o.(T), phase, mod)
	}
}

func (p *phases[I, T, E]) Process(o InternalObject, req model.Request) model.ProcessingResult {
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

func (p *phases[I, T, E]) PrepareDeletion(lctx model.Logging, ob objectbase.Objectbase, o InternalObject, phase mmids.Phase) error {
	ph := p.phases[phase]
	if ph != nil {
		log := lctx.Logger(p.realm).WithValues("name", o.GetName(), "phase", phase)
		return ph.PrepareDeletion(log, ob, o.(I), phase)
	}
	return nil
}

////////////////////////////////////////////////////////////////////////////////

// InternalPhaseObjectSupport provides complete support for
// [model.InternalObject] by using [Phases].
// It requires the effective [model.InternalObject] to implement
// [runtime.InitializedObject] to init the Phases attribute
type InternalPhaseObjectSupport[I InternalObject, T db.InternalDBObject, E model.ExternalState] struct {
	InternalObjectSupport[T] `json:",inline"`
	self                     I
	phases                   Phases[I, T, E] `json:",omitempty"`
}

var _ model.InternalObject = (*InternalPhaseObjectSupport[InternalObject, db.InternalDBObject, model.ExternalState])(nil)

type selfer[I InternalObject, T db.InternalDBObject, E model.ExternalState] interface {
	setSelf(i I, phases Phases[I, T, E], pi PhaseStateAccess[T])
}

func SetSelf[I InternalObject, T db.InternalDBObject, E model.ExternalState](i I, phases Phases[I, T, E], phaseInfos PhaseStateAccess[T]) error {
	o, ok := utils.TryCast[selfer[I, T, E]](i)
	if !ok {
		return fmt.Errorf("invalid object type %T", i)
	}
	o.setSelf(i, phases, phaseInfos)
	return nil
}

func (n *InternalPhaseObjectSupport[I, T, E]) setSelf(i I, phases Phases[I, T, E], pi PhaseStateAccess[T]) {
	n.phases = phases
	n.self = i
	n.phaseInfos = pi
}

func (n *InternalPhaseObjectSupport[I, T, E]) GetDBObject() T {
	return utils.Cast[T](n.GetBase())
}

func (n *InternalPhaseObjectSupport[I, T, E]) GetExternalState(ext model.ExternalObject, phase mmids.Phase) model.ExternalState {
	return n.phases.GetExternalState(n.self, ext, phase)
}

func (n *InternalPhaseObjectSupport[I, T, E]) GetCurrentState(phase mmids.Phase) model.CurrentState {
	return n.phases.GetCurrentState(n.self, phase)
}

func (n *InternalPhaseObjectSupport[I, T, E]) GetTargetState(phase mmids.Phase) model.TargetState {
	return n.phases.GetTargetState(n.self, phase)
}

func (n *InternalPhaseObjectSupport[I, T, E]) AcceptExternalState(lctx model.Logging, ob objectbase.Objectbase, phase mmids.Phase, states model.ExternalStates) (model.AcceptStatus, error) {
	n.Lock.Lock()
	defer n.Lock.Unlock()

	status, err := n.phases.AcceptExternalState(lctx, n.self, phase, states)
	if status != model.ACCEPT_OK || err != nil {
		return status, err
	}
	mod := func(_o db.DBObject) (bool, bool) {
		mod := false
		for _, s := range states {
			n.phases.DBSetExternalState(lctx, n, _o.(db.InternalDBObject), phase, s.(E), &mod)
		}
		return mod, mod
	}
	_, err = wrapped.Modify(ob, n, mod)
	return model.ACCEPT_OK, err
}

func (n *InternalPhaseObjectSupport[I, T, E]) PrepareDeletion(lctx model.Logging, ob objectbase.Objectbase, phase mmids.Phase) error {
	return n.phases.PrepareDeletion(lctx, ob, n.self, phase)
}

func (n *InternalPhaseObjectSupport[I, T, E]) Process(request model.Request) model.ProcessingResult {
	return n.phases.Process(n.self, request)
}

func (n *InternalPhaseObjectSupport[I, T, E]) Rollback(lctx model.Logging, ob objectbase.Objectbase, phase mmids.Phase, id mmids.RunId, observed ...string) (bool, error) {
	n.Lock.Lock()
	defer n.Lock.Unlock()

	mod := func(_o db.DBObject) (bool, bool) {
		o := _o.(T)
		v := utils.Optional(observed...)
		p := n.GetPhaseStateFor(o, phase)
		b := p.ClearLock(id)
		if b {
			if v != "" {
				p.GetCurrent().SetObservedVersion(v)
			}
			p.ClearTarget()
			n.phases.DBRollback(lctx, o, phase, &b)
		}
		return b, b
	}
	return wrapped.Modify(ob, n, mod)
}

func (n *InternalPhaseObjectSupport[I, T, E]) Commit(lctx model.Logging, ob objectbase.Objectbase, phase mmids.Phase, id mmids.RunId, commit *model.CommitInfo) (bool, error) {

	f := CommitFunc[T](func(lctx model.Logging, o T, phase mmids.Phase, spec *model.CommitInfo) {
		var b bool
		n.phases.DBCommit(lctx, o, phase, commit, &b)
	})
	return n.InternalObjectSupport.HandleCommit(lctx, ob, phase, id, commit, f)
}
