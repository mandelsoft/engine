package support

import (
	"fmt"
	"sync"

	"github.com/mandelsoft/engine/pkg/database"
	"github.com/mandelsoft/engine/pkg/processing/metamodel/objectbase"
	"github.com/mandelsoft/engine/pkg/processing/metamodel/objectbase/wrapped"
	"github.com/mandelsoft/engine/pkg/processing/mmids"
	"github.com/mandelsoft/engine/pkg/processing/model"
	"github.com/mandelsoft/engine/pkg/runtime"
	"github.com/mandelsoft/engine/pkg/utils"
	"github.com/mandelsoft/logging"
)

type _ = runtime.InitializedObject // use runtime package for go doc

type Phase[I InternalObject, T InternalDBObject, E model.ExternalState] interface {
	DBCommit(log logging.Logger, o T, phase mmids.Phase, spec *model.CommitInfo, mod *bool)
	DBSetExternalState(log logging.Logger, o T, phase mmids.Phase, state E, mod *bool)

	GetExternalState(o I, ext model.ExternalObject, phase mmids.Phase) model.ExternalState
	GetCurrentState(o I, phase mmids.Phase) model.CurrentState
	GetTargetState(o I, phase mmids.Phase) model.TargetState
	Process(o I, phase mmids.Phase, req model.Request) model.Status
}

type Phases[I InternalObject, T InternalDBObject, E model.ExternalState] interface {
	Register(name mmids.Phase, ph Phase[I, T, E])

	DBCommit(lctx model.Logging, _o InternalDBObject, phase mmids.Phase, commit *model.CommitInfo, mod *bool)
	DBSetExternalState(lctx model.Logging, _o InternalDBObject, phase mmids.Phase, s model.ExternalState, mod *bool)

	GetExternalState(o InternalObject, ext model.ExternalObject, phase mmids.Phase) model.ExternalState
	GetCurrentState(o InternalObject, phase mmids.Phase) model.CurrentState
	GetTargetState(o InternalObject, phase mmids.Phase) model.TargetState
	Process(o InternalObject, req model.Request) model.Status
}

type DefaultPhase[I InternalObject] struct{}

func (p DefaultPhase[I]) GetExternalState(o I, ext model.ExternalObject, phase mmids.Phase) model.ExternalState {
	return ext.GetState()
}

type phases[I InternalObject, T InternalDBObject, E model.ExternalState] struct {
	realm  logging.Realm
	phases map[mmids.Phase]Phase[I, T, E]
}

func NewPhases[I InternalObject, T InternalDBObject, E model.ExternalState](realm logging.Realm) Phases[I, T, E] {
	return &phases[I, T, E]{
		realm,
		map[mmids.Phase]Phase[I, T, E]{},
	}
}

func (p *phases[I, T, E]) Register(name mmids.Phase, ph Phase[I, T, E]) {
	p.phases[name] = ph
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

func (p *phases[I, T, E]) DBCommit(lctx model.Logging, _o InternalDBObject, phase mmids.Phase, commit *model.CommitInfo, mod *bool) {
	ph := p.phases[phase]
	if ph != nil {
		log := lctx.Logger(p.realm).WithValues("name", _o.GetName(), "phase", phase)
		ph.DBCommit(log, _o.(T), phase, commit, mod)
	}
}

func (p *phases[I, T, E]) DBSetExternalState(lctx model.Logging, _o InternalDBObject, phase mmids.Phase, s model.ExternalState, mod *bool) {
	ph := p.phases[phase]
	if ph != nil {
		log := lctx.Logger(p.realm).WithValues("name", _o.GetName(), "phase", phase)
		ph.DBSetExternalState(log, _o.(T), phase, s.(E), mod)
	}
}

func (p *phases[I, T, E]) Process(o InternalObject, req model.Request) model.Status {
	phase := req.Element.GetPhase()
	ph := p.phases[phase]
	if ph != nil {
		req.Logging = req.Logging.WithContext(p.realm)
		return ph.Process(o.(I), phase, req)
	}
	return model.Status{
		Status: model.STATUS_FAILED,
		Error:  fmt.Errorf("unknown phase %q", phase),
	}
}

////////////////////////////////////////////////////////////////////////////////

// InternalPhaseObjectSupport provides complete support for
// [model.InternalObject] by using [Phases].
// It requires the effective [model.InternalObject] to implement
// [runtime.InitializedObject] to init the Phases attribute
type InternalPhaseObjectSupport[I InternalObject, T InternalDBObject, E model.ExternalState] struct {
	Lock sync.Mutex
	Wrapper
	self   I
	phases Phases[I, T, E] `json:",omitempty"`
}

var _ model.InternalObject = (*InternalPhaseObjectSupport[InternalObject, InternalDBObject, model.ExternalState])(nil)

type selfer[I InternalObject, T InternalDBObject, E model.ExternalState] interface {
	setSelf(i I, phases Phases[I, T, E])
}

func SetSelf[I InternalObject, T InternalDBObject, E model.ExternalState](i I, phases Phases[I, T, E]) error {
	o, ok := utils.TryCast[selfer[I, T, E]](i)
	if !ok {
		return fmt.Errorf("invalid object type %T", i)
	}
	o.setSelf(i, phases)
	return nil
}

func (n *InternalPhaseObjectSupport[I, T, E]) setSelf(i I, phases Phases[I, T, E]) {
	n.phases = phases
	n.self = i
}

func (n *InternalPhaseObjectSupport[I, T, E]) GetDatabase(ob objectbase.Objectbase) database.Database[DBObject] {
	return objectbase.GetDatabase[DBObject](ob)
}

func (n *InternalPhaseObjectSupport[I, T, E]) GetDBObject() InternalDBObject {
	return utils.Cast[InternalDBObject](n.GetBase())
}

func (n *InternalPhaseObjectSupport[I, T, E]) GetLock(phase mmids.Phase) mmids.RunId {
	n.Lock.Lock()
	defer n.Lock.Unlock()
	return n.GetDBObject().GetLock(phase)
}

func (n *InternalPhaseObjectSupport[I, T, E]) TryLock(ob objectbase.Objectbase, phase mmids.Phase, id mmids.RunId) (bool, error) {
	n.Lock.Lock()
	defer n.Lock.Unlock()

	mod := func(o DBObject) (bool, bool) {
		b := utils.Cast[InternalDBObject](o).TryLock(phase, id)
		return b, b
	}
	return wrapped.Modify(ob, n, mod)
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

func (n *InternalPhaseObjectSupport[I, T, E]) SetExternalState(lctx model.Logging, ob objectbase.Objectbase, phase mmids.Phase, states model.ExternalStates) error {
	n.Lock.Lock()
	defer n.Lock.Unlock()

	mod := func(_o DBObject) (bool, bool) {
		mod := false
		for _, s := range states {
			n.phases.DBSetExternalState(lctx, _o.(InternalDBObject), phase, s.(E), &mod)
		}
		return mod, mod
	}
	_, err := wrapped.Modify(ob, n, mod)
	return err
}

func (n *InternalPhaseObjectSupport[I, T, E]) Process(request model.Request) model.Status {
	return n.phases.Process(n.self, request)
}

func (n *InternalPhaseObjectSupport[I, T, E]) Rollback(lctx model.Logging, ob objectbase.Objectbase, phase mmids.Phase, id mmids.RunId) (bool, error) {
	n.Lock.Lock()
	defer n.Lock.Unlock()

	mod := func(_o DBObject) (bool, bool) {
		o := utils.Cast[InternalDBObject](_o)
		b := o.ClearLock(phase, id)
		return b, b
	}
	return wrapped.Modify(ob, n, mod)
}

func (n *InternalPhaseObjectSupport[I, T, E]) Commit(lctx model.Logging, ob objectbase.Objectbase, phase mmids.Phase, id mmids.RunId, commit *model.CommitInfo) (bool, error) {
	n.Lock.Lock()
	defer n.Lock.Unlock()

	mod := func(_o DBObject) (bool, bool) {
		o := utils.Cast[InternalDBObject](_o)
		b := o.ClearLock(phase, id)
		if b {
			n.phases.DBCommit(lctx, o, phase, commit, &b)
		}
		return b, b
	}
	return wrapped.Modify(ob, n, mod)
}