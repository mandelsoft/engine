package support

import (
	"fmt"
	"sync"

	"github.com/mandelsoft/engine/pkg/database"
	"github.com/mandelsoft/engine/pkg/processing/internal"
	"github.com/mandelsoft/engine/pkg/processing/mmids"
	"github.com/mandelsoft/engine/pkg/processing/model"
	"github.com/mandelsoft/engine/pkg/processing/model/support/db"
	"github.com/mandelsoft/engine/pkg/processing/objectbase"
	"github.com/mandelsoft/engine/pkg/processing/objectbase/wrapped"
	"github.com/mandelsoft/engine/pkg/utils"
	"github.com/mandelsoft/goutils/generics"
	"github.com/mandelsoft/goutils/matcher"
	"github.com/mandelsoft/goutils/sliceutils"
)

// PhaseStateAccessFunc is the replacement for a C++ member pointer.
// It describes the access to a dedicated [PhaseState] field
// in a state object according to the type parameter.
type PhaseStateAccessFunc[I db.InternalDBObject] func(I) db.PhaseState

////////////////////////////////////////////////////////////////////////////////

type PhaseStateAccess[I db.InternalDBObject] map[mmids.Phase]PhaseStateAccessFunc[I]

func NewPhaseStateAccess[I db.InternalDBObject]() PhaseStateAccess[I] {
	return PhaseStateAccess[I]{}
}

func (p PhaseStateAccess[I]) Register(phase mmids.Phase, infoFunc PhaseStateAccessFunc[I]) {
	p[phase] = infoFunc
}

func (n PhaseStateAccess[I]) GetPhaseState(o I, phase mmids.Phase) db.PhaseState {
	f := n[phase]
	if f == nil {
		panic(fmt.Sprintf("invalid phase %s for type %s[%T]", phase, o.GetType(), o))
	}
	return f(o)
}

////////////////////////////////////////////////////////////////////////////////

type pointer[P any] interface {
	db.InternalDBObject
	*P
}

type InternalObject interface {
	model.InternalObject
	GetBase() db.Object
	GetPhaseState(phase mmids.Phase) db.PhaseState
	GetPhaseStateFor(o db.InternalDBObject, phase mmids.Phase) db.PhaseState
}

type InternalObjectSupport[I db.InternalDBObject] struct {
	Lock sync.Mutex
	Wrapper
	phaseInfos PhaseStateAccess[I]
}

type phaser[T db.InternalDBObject] interface {
	setPhaseStateAccess(pi PhaseStateAccess[T])
}

func SetPhaseStateAccess[T db.InternalDBObject](i InternalObject, phaseInfos PhaseStateAccess[T]) error {
	o, ok := generics.TryCast[phaser[T]](i)
	if !ok {
		return fmt.Errorf("invalid object type %T", i)
	}
	o.setPhaseStateAccess(phaseInfos)
	return nil
}

// setPhaseInfos must be called by an init function of the
// final internal object type to set up the
// phase info access functions.
func (n *InternalObjectSupport[I]) setPhaseStateAccess(pi PhaseStateAccess[I]) {
	n.phaseInfos = pi
}

func (n *InternalObjectSupport[I]) GetDBObject() I {
	return n.GetBase().(I)
}

func (n *InternalObjectSupport[I]) GetPhaseState(phase mmids.Phase) db.PhaseState {
	return n.phaseInfos.GetPhaseState(n.GetDBObject(), phase)
}

func (n *InternalObjectSupport[I]) GetPhaseStateFor(o db.InternalDBObject, phase mmids.Phase) db.PhaseState {
	return n.phaseInfos.GetPhaseState(o.(I), phase)
}

// GetExternalState is a default implementation just forwarding
// the external state as provided by the external object.
func (n *InternalObjectSupport[I]) GetExternalState(o model.ExternalObject, phase mmids.Phase) model.ExternalState {
	return o.GetState()
}

func (n *InternalObjectSupport[I]) GetDatabase(ob objectbase.Objectbase) database.Database[db.Object] {
	return objectbase.GetDatabase[db.Object](ob)
}

func (n *InternalObjectSupport[I]) GetStatus(phase mmids.Phase) model.Status {
	n.Lock.Lock()
	defer n.Lock.Unlock()
	return n.GetPhaseState(phase).GetStatus()
}

func (n *InternalObjectSupport[I]) SetStatus(ob objectbase.Objectbase, phase mmids.Phase, status model.Status) (bool, error) {
	n.Lock.Lock()
	defer n.Lock.Unlock()

	mod := func(o db.Object) (bool, bool) {
		b := n.GetPhaseState(phase).SetStatus(status)
		return b, b
	}
	return wrapped.Modify(ob, n, mod)
}

func (n *InternalObjectSupport[I]) GetLock(phase mmids.Phase) mmids.RunId {
	n.Lock.Lock()
	defer n.Lock.Unlock()
	return n.GetPhaseState(phase).GetLock()
}

func (n *InternalObjectSupport[I]) TryLock(ob objectbase.Objectbase, phase mmids.Phase, id mmids.RunId) (bool, error) {
	n.Lock.Lock()
	defer n.Lock.Unlock()

	mod := func(o db.Object) (bool, bool) {
		b := n.GetPhaseState(phase).TryLock(id)
		return b, b
	}
	return wrapped.Modify(ob, n, mod)
}

type Rollbacker[P any] interface {
	DBRollback(lctx model.Logging, o P, phase mmids.Phase)
}

type RollbackFunc[P any] func(lctx model.Logging, o P, phase mmids.Phase)

func (f RollbackFunc[P]) DBRollback(lctx model.Logging, o P, phase mmids.Phase) {
	f(lctx, o, phase)
}

func (n *InternalObjectSupport[I]) HandleRollback(lctx model.Logging, ob objectbase.Objectbase, phase mmids.Phase, id mmids.RunId, tgt model.TargetState, formal *string, rollbacker Rollbacker[I]) (bool, error) {
	n.Lock.Lock()
	defer n.Lock.Unlock()

	log := lctx.Logger()
	mod := func(_o db.Object) (bool, bool) {
		o := _o.(I)
		p := n.GetPhaseStateFor(_o.(I), phase)
		b := p.ClearLock(id)
		if b {
			log.Info("  runlock {{runid}} cleared", "runid", id)
			if tgt != nil {
				log.Info("  setting observed state {{observed}}", "observed", p.GetTarget().GetObjectVersion())
				p.GetCurrent().SetObservedVersion(p.GetTarget().GetObjectVersion())
			}
			if formal != nil {
				log.Info("  setting formal version {{formal}}", "formal", *formal)
				p.GetCurrent().SetFormalVersion(*formal)
			}
			if rollbacker != nil {
				rollbacker.DBRollback(lctx, o, phase)
			}
			p.ClearTarget()
		} else {
			log.Error("{{element}} not locked for {{runid}} (found {{busy}})", "runid", id, "busy", p.GetLock())
		}
		return b, b
	}
	return wrapped.Modify(ob, n, mod)
}

func (n *InternalObjectSupport[I]) MarkPhasesForDeletion(ob objectbase.Objectbase, phases ...mmids.Phase) (bool, error) {
	n.Lock.Lock()
	defer n.Lock.Unlock()

	mod := func(_o db.Object) (bool, bool) {
		mod := false
		t := utils.NewTimestamp()
		for _, phase := range phases {
			p := n.GetPhaseStateFor(_o.(I), phase)
			mod = p.MarkForDeletion(t) || mod
		}
		return mod, mod
	}
	return wrapped.Modify(ob, n, mod)
}
func (n *InternalObjectSupport[I]) IsMarkedForDeletion(phase mmids.Phase) bool {
	n.Lock.Lock()
	defer n.Lock.Unlock()

	return n.GetPhaseState(phase).IsDeletionRequested()
}

type Committer[P any] interface {
	DBCommit(lctx model.Logging, o P, phase mmids.Phase, spec *model.CommitInfo)
}

type CommitFunc[P any] func(lctx model.Logging, o P, phase mmids.Phase, spec *model.CommitInfo)

func (f CommitFunc[P]) DBCommit(lctx model.Logging, o P, phase mmids.Phase, spec *model.CommitInfo) {
	f(lctx, o, phase, spec)
}

func (n *InternalObjectSupport[I]) HandleCommit(lctx model.Logging, ob objectbase.Objectbase, phase mmids.Phase, id mmids.RunId, commit *model.CommitInfo, committer Committer[I]) (bool, error) {
	n.Lock.Lock()
	defer n.Lock.Unlock()

	log := lctx.Logger()
	mod := func(_o db.Object) (bool, bool) {
		log.Info("Commit target state for {{element}}")
		o := _o.(I)
		p := n.GetPhaseStateFor(o, phase)
		b := p.ClearLock(id)
		if b {
			log.Info("  runlock {{runid}} cleared", "runid", id)
			if commit != nil {
				c := p.GetCurrent()
				log.Info("  input version {{input}}", "input", commit.InputVersion)
				c.SetInputVersion(commit.InputVersion)
				v := p.GetTarget().GetObjectVersion()
				if commit.ObjectVersion != nil && v != *commit.ObjectVersion {
					log.Info("  modified object version {{object}} (original {{orig}})", "object", *commit.ObjectVersion, "orig", v)
					v = *commit.ObjectVersion
				} else {
					log.Info("  object version {{object}}", "object", v)
				}
				c.SetObjectVersion(v)
				log.Info("  observed version {{observed}}", "observed", v)
				c.SetObservedVersion(v)
				v = commit.OutputState.GetOutputVersion()
				log.Info("  output version {{output}}", "output", v)
				c.SetOutputVersion(v)
				v = commit.OutputState.GetFormalVersion()
				log.Info("  formal version {{formal}}", "formal", v)
				c.SetFormalVersion(v)
			}
			if committer != nil {
				committer.DBCommit(lctx, o, phase, commit)
			}
			p.ClearTarget()
		} else {
			log.Error("{{element}} not locked for {{runid}} (found {{busy}})", "runid", id, "busy", p.GetLock())
		}
		return b, b
	}
	return wrapped.Modify(ob, n, mod)
}

func (n *InternalObjectSupport[I]) PrepareDeletion(lctx internal.Logging, mgmt internal.SlaveManagement, phase mmids.Phase) error {
	return nil
}

////////////////////////////////////////////////////////////////////////////////

type stateSupportBase[I db.InternalDBObject] struct {
	phase mmids.Phase
	io    InternalObject
}

func (c *stateSupportBase[I]) GetType() string {
	return c.io.GetType()
}

func (c *stateSupportBase[I]) GetName() string {
	return c.io.GetName()
}

func (c *stateSupportBase[I]) GetNamespace() string {
	return c.io.GetNamespace()
}

func (c *stateSupportBase[I]) GetDBObject() I {
	return c.io.GetBase().(I)
}

func (c *stateSupportBase[I]) PhaseLink(phase mmids.Phase) mmids.ElementId {
	return mmids.NewElementId(c.GetType(), c.GetNamespace(), c.GetName(), phase)
}

func (c *stateSupportBase[I]) SlaveLink(typ string, phase mmids.Phase) mmids.ElementId {
	return mmids.NewElementId(typ, c.GetNamespace(), c.GetName(), phase)
}

func (c *stateSupportBase[I]) SlaveLinkFor(id mmids.TypeId) mmids.ElementId {
	return mmids.NewElementId(id.GetType(), c.GetNamespace(), c.GetName(), id.GetPhase())
}

func (c *stateSupportBase[I]) GetPhaseInfo() db.PhaseState {
	return c.io.GetPhaseState(c.phase)
}

type CurrentStateSupport[I db.InternalDBObject, C db.CurrentState] struct {
	stateSupportBase[I]
}

func NewCurrentStateSupport[I db.InternalDBObject, C db.CurrentState](o InternalObject, phase mmids.Phase) CurrentStateSupport[I, C] {
	return CurrentStateSupport[I, C]{
		stateSupportBase[I]{
			phase: phase,
			io:    o,
		},
	}
}

func (c *CurrentStateSupport[I, C]) Get() C {
	return c.GetPhaseInfo().GetCurrent().(C)
}

func (c *CurrentStateSupport[I, C]) GetObservedStateForTypeAndPhase(typ string, phase mmids.Phase, names ...string) model.ObservedState {
	return NewDefaultObservedStateForTypePhase(c.GetObservedVersion(), typ, c.GetNamespace(), phase, names...)
}

func (c *CurrentStateSupport[I, C]) GetObservedStateForPhase(phase mmids.Phase, add ...mmids.ElementId) model.ObservedState {
	s := NewDefaultObservedStateForTypePhase(c.GetObservedVersion(), c.GetType(), c.GetNamespace(), phase, c.GetName())
	s.(*defaultObservedState).links = append(s.(*defaultObservedState).links, add...)
	return s
}

func (c *CurrentStateSupport[I, C]) GetObservedVersion() string {
	return c.Get().GetObservedVersion()
}

func (c *CurrentStateSupport[I, C]) GetFormalVersion() string {
	return c.Get().GetFormalVersion()
}

func (c *CurrentStateSupport[I, C]) GetInputVersion() string {
	return c.Get().GetInputVersion()
}

func (c *CurrentStateSupport[I, C]) GetObjectVersion() string {
	return c.Get().GetObjectVersion()
}

func (c *CurrentStateSupport[I, C]) GetOutputVersion() string {
	return c.Get().GetOutputVersion()
}

////////////////////////////////////////////////////////////////////////////////

type TargetStateSupport[I db.InternalDBObject, T db.TargetState] struct {
	stateSupportBase[I]
}

func NewTargetStateSupport[I db.InternalDBObject, T db.TargetState](o InternalObject, phase mmids.Phase) TargetStateSupport[I, T] {
	return TargetStateSupport[I, T]{
		stateSupportBase[I]{
			phase: phase,
			io:    o,
		},
	}
}

func (c *TargetStateSupport[I, T]) Get() T {
	return c.GetPhaseInfo().GetTarget().(T)
}

func (c *TargetStateSupport[I, T]) GetFormalObjectVersion() string {
	return c.Get().GetFormalObjectVersion()
}

func (c *TargetStateSupport[I, T]) GetInputVersion(inputs model.Inputs) string {
	return DefaultInputVersion(inputs)
}

func (c *TargetStateSupport[I, T]) GetObjectVersion() string {
	return c.Get().GetObjectVersion()
}

////////////////////////////////////////////////////////////////////////////////

type defaultObservedState struct {
	version string
	links   []mmids.ElementId
}

var _ model.ObservedState = (*defaultObservedState)(nil)

func NewDefaultObservedState(v string, links []mmids.ElementId) model.ObservedState {
	return &defaultObservedState{v, links}
}

func NewDefaultObservedStateForTypePhase(v string, typ string, namespace string, phase mmids.Phase, names ...string) model.ObservedState {
	return &defaultObservedState{v, LinksForTypePhase(typ, namespace, phase, names...)}
}

func LinksForTypePhase(typ string, namespace string, phase mmids.Phase, names ...string) []mmids.ElementId {
	var links []mmids.ElementId
	for _, l := range sliceutils.Filter(names, matcher.NotInitial[string]) {
		links = append(links, mmids.NewElementId(typ, namespace, l, phase))
	}
	return links
}

func (d *defaultObservedState) GetObjectVersion() string {
	return d.version
}

func (d *defaultObservedState) GetLinks() []mmids.ElementId {
	return d.links
}
