package support

import (
	"fmt"
	"sync"

	"github.com/mandelsoft/engine/pkg/database"
	"github.com/mandelsoft/engine/pkg/metamodel/common"
	"github.com/mandelsoft/engine/pkg/metamodel/model"
	"github.com/mandelsoft/engine/pkg/metamodel/objectbase"
	"github.com/mandelsoft/engine/pkg/metamodel/objectbase/wrapped"
	"github.com/mandelsoft/engine/pkg/runtime"
	"github.com/mandelsoft/engine/pkg/utils"
	"github.com/mandelsoft/logging"
)

type _ = runtime.InitializedObject // use runtime package for go doc

type Phase[T InternalDBObject, E common.ExternalState] interface {
	DBCommit(lctx common.Logging, o T, phase model.Phase, spec *model.CommitInfo, mod *bool)
	DBSetExternalState(lcxt common.Logging, o T, phase model.Phase, state E, mod *bool)

	GetCurrentState(o InternalObject, phase model.Phase) model.CurrentState
	GetTargetState(o InternalObject, phase model.Phase) model.TargetState
	Process(ob objectbase.Objectbase, o InternalObject, phase model.Phase, req model.Request) model.Status
}

type Phases[T InternalDBObject, E common.ExternalState] interface {
	Register(name model.Phase, ph Phase[T, E])

	DBCommit(lctx common.Logging, _o InternalDBObject, phase model.Phase, commit *model.CommitInfo, mod *bool)
	DBSetExternalState(lctx common.Logging, _o InternalDBObject, phase model.Phase, s common.ExternalState, mod *bool)

	GetCurrentState(o InternalObject, phase model.Phase) model.CurrentState
	GetTargetState(o InternalObject, phase model.Phase) model.TargetState
	Process(ob objectbase.Objectbase, o InternalObject, req model.Request) model.Status
}

type phases[T InternalDBObject, E common.ExternalState] struct {
	realm  logging.Realm
	phases map[model.Phase]Phase[T, E]
}

func NewPhases[T InternalDBObject, E common.ExternalState](realm logging.Realm) Phases[T, E] {
	return &phases[T, E]{
		realm,
		map[model.Phase]Phase[T, E]{},
	}
}

func (p *phases[T, E]) Register(name model.Phase, ph Phase[T, E]) {
	p.phases[name] = ph
}

func (p *phases[T, E]) GetCurrentState(o InternalObject, phase model.Phase) model.CurrentState {
	ph := p.phases[phase]
	if ph != nil {
		return ph.GetCurrentState(o, phase)
	}
	return nil
}

func (p *phases[T, E]) GetTargetState(o InternalObject, phase model.Phase) model.TargetState {
	ph := p.phases[phase]
	if ph != nil {
		return ph.GetTargetState(o, phase)
	}
	return nil
}

func (p *phases[T, E]) DBCommit(lctx common.Logging, _o InternalDBObject, phase model.Phase, commit *model.CommitInfo, mod *bool) {
	ph := p.phases[phase]
	if ph != nil {
		lctx = lctx.WithContext(p.realm)
		ph.DBCommit(lctx, _o.(T), phase, commit, mod)
	}
}

func (p *phases[T, E]) DBSetExternalState(lctx common.Logging, _o InternalDBObject, phase model.Phase, s common.ExternalState, mod *bool) {
	ph := p.phases[phase]
	if ph != nil {
		lctx = lctx.WithContext(p.realm)
		ph.DBSetExternalState(lctx, _o.(T), phase, s.(E), mod)
	}
}

func (p *phases[T, E]) Process(ob objectbase.Objectbase, o InternalObject, req model.Request) model.Status {
	phase := req.Element.GetPhase()
	ph := p.phases[phase]
	if ph != nil {
		req.Logging = req.Logging.WithContext(p.realm)
		return ph.Process(ob, o, phase, req)
	}
	return model.Status{
		Status: common.STATUS_FAILED,
		Error:  fmt.Errorf("unknown phase %q", phase),
	}
}

////////////////////////////////////////////////////////////////////////////////

// InternalPhaseObjectSupport provides complete support for
// [model.InternalObject] by using [Phases].
// It requires the effective [model.InternalObject] to implement
// [runtime.InitializedObject] to init the Phases attribute
type InternalPhaseObjectSupport[T InternalDBObject, E common.ExternalState] struct {
	Lock sync.Mutex
	Wrapper
	Phases Phases[T, E] `json:",omitempty"`
}

var _ model.InternalObject = (*InternalPhaseObjectSupport)(nil)

func (n *InternalPhaseObjectSupport[T, E]) GetDatabase(ob objectbase.Objectbase) database.Database[DBObject] {
	return objectbase.GetDatabase[DBObject](ob)
}

func (n *InternalPhaseObjectSupport[T, E]) GetDBObject() InternalDBObject {
	return utils.Cast[InternalDBObject](n.GetBase())
}

func (n *InternalPhaseObjectSupport[T, E]) GetLock(phase common.Phase) common.RunId {
	n.Lock.Lock()
	defer n.Lock.Unlock()
	return n.GetDBObject().GetLock(phase)
}

func (n *InternalPhaseObjectSupport[T, E]) TryLock(ob objectbase.Objectbase, phase common.Phase, id common.RunId) (bool, error) {
	n.Lock.Lock()
	defer n.Lock.Unlock()

	mod := func(o DBObject) (bool, bool) {
		b := utils.Cast[InternalDBObject](o).TryLock(phase, id)
		return b, b
	}
	return wrapped.Modify(ob, n, mod)
}

func (n *InternalPhaseObjectSupport[T, E]) GetCurrentState(phase common.Phase) common.CurrentState {
	return n.Phases.GetCurrentState(n, phase)
}

func (n *InternalPhaseObjectSupport[T, E]) GetTargetState(phase common.Phase) common.TargetState {
	return n.Phases.GetTargetState(n, phase)
}

func (n *InternalPhaseObjectSupport[T, E]) SetExternalState(lctx common.Logging, ob common.Objectbase, phase common.Phase, states common.ExternalStates) error {
	n.Lock.Lock()
	defer n.Lock.Unlock()

	mod := func(_o DBObject) (bool, bool) {
		mod := false
		for _, s := range states {
			n.Phases.DBSetExternalState(lctx, _o.(InternalDBObject), phase, s.(E), &mod)
		}
		return mod, mod
	}
	_, err := wrapped.Modify(ob, n, mod)
	return err
}

func (n *InternalPhaseObjectSupport[T, E]) Process(ob common.Objectbase, request common.Request) common.Status {
	return n.Phases.Process(ob, n, request)
}

func (n *InternalPhaseObjectSupport[T, E]) Rollback(lctx common.Logging, ob objectbase.Objectbase, phase common.Phase, id model.RunId) (bool, error) {
	n.Lock.Lock()
	defer n.Lock.Unlock()

	mod := func(_o DBObject) (bool, bool) {
		o := utils.Cast[InternalDBObject](_o)
		b := o.ClearLock(phase, id)
		return b, b
	}
	return wrapped.Modify(ob, n, mod)
}

func (n *InternalPhaseObjectSupport[T, E]) Commit(lctx common.Logging, ob objectbase.Objectbase, phase common.Phase, id model.RunId, commit *model.CommitInfo) (bool, error) {
	n.Lock.Lock()
	defer n.Lock.Unlock()

	mod := func(_o DBObject) (bool, bool) {
		o := utils.Cast[InternalDBObject](_o)
		b := o.ClearLock(phase, id)
		if b {
			n.Phases.DBCommit(lctx, o, phase, commit, &b)
		}
		return b, b
	}
	return wrapped.Modify(ob, n, mod)
}
