package support

import (
	"sync"

	"github.com/mandelsoft/engine/pkg/database"
	"github.com/mandelsoft/engine/pkg/processing/metamodel/objectbase"
	"github.com/mandelsoft/engine/pkg/processing/metamodel/objectbase/wrapped"
	"github.com/mandelsoft/engine/pkg/processing/mmids"
	"github.com/mandelsoft/engine/pkg/processing/model"
	"github.com/mandelsoft/engine/pkg/utils"
)

type ElementInfo interface {
	ClearLock(mmids.RunId) bool
	GetLock() mmids.RunId
	TryLock(mmids.RunId) bool

	GetStatus() model.Status
	SetStatus(model.Status) bool
}

type DefaultElementInfo struct {
	RunId  mmids.RunId  `json:"runid"`
	Status model.Status `json:"status"`
}

var _ ElementInfo = (*DefaultElementInfo)(nil)

func (n *DefaultElementInfo) ClearLock(id mmids.RunId) bool {
	if n.RunId != id {
		return false
	}
	n.RunId = ""
	return true
}

func (n *DefaultElementInfo) GetLock() mmids.RunId {
	return n.RunId
}

func (n *DefaultElementInfo) TryLock(id mmids.RunId) bool {
	if n.RunId != "" && n.RunId != id {
		return false
	}
	n.RunId = id
	return true
}

func (n *DefaultElementInfo) GetStatus() model.Status {
	return n.Status
}

func (n *DefaultElementInfo) SetStatus(s model.Status) bool {
	if n.Status == s {
		return false
	}
	n.Status = s
	return true
}

// ///////////////////////////////
type pointer[P any] interface {
	ElementInfo
	*P
}

type ElementInfos[P pointer[E], E any] struct {
	PhaseInfos map[mmids.Phase]E `json:"phaseInfos,omitempty"`
}

func (n *ElementInfos[P, E]) ClearLock(phase mmids.Phase, id mmids.RunId) bool {
	if len(n.PhaseInfos) == 0 {
		return false
	}
	if n.PhaseInfos == nil {
		n.PhaseInfos = map[mmids.Phase]E{}
	}
	i, ok := n.PhaseInfos[phase]
	if !ok {
		return false
	}
	ok = utils.Cast[P](&i).ClearLock(id)
	if ok {
		n.PhaseInfos[phase] = i
	}
	return ok
}

func (n *ElementInfos[P, E]) GetLock(phase mmids.Phase) mmids.RunId {
	if len(n.PhaseInfos) == 0 {
		return ""
	}
	if n.PhaseInfos == nil {
		n.PhaseInfos = map[mmids.Phase]E{}
	}
	i := n.PhaseInfos[phase]
	return P(&i).GetLock()
}

func (n *ElementInfos[P, E]) TryLock(phase mmids.Phase, id mmids.RunId) bool {
	if n.PhaseInfos == nil {
		n.PhaseInfos = map[mmids.Phase]E{}
	}
	i := n.PhaseInfos[phase]
	ok := P(&i).TryLock(id)
	if ok {
		n.PhaseInfos[phase] = i
	}
	return ok
}

func (n *ElementInfos[P, E]) GetStatus(phase mmids.Phase) model.Status {
	if len(n.PhaseInfos) == 0 {
		return ""
	}
	if n.PhaseInfos == nil {
		n.PhaseInfos = map[mmids.Phase]E{}
	}
	i := n.PhaseInfos[phase]
	return P(&i).GetStatus()
}

func (n *ElementInfos[P, E]) SetStatus(phase mmids.Phase, status model.Status) bool {
	if n.PhaseInfos == nil {
		n.PhaseInfos = map[mmids.Phase]E{}
	}
	i := n.PhaseInfos[phase]
	ok := P(&i).SetStatus(status)
	if ok {
		n.PhaseInfos[phase] = i
	}
	return ok
}

type InternalDBObjectSupport[P pointer[E], E any] struct {
	database.GenerationObjectMeta

	ElementInfos[P, E]
}

type E = ElementInfos[*DefaultElementInfo, DefaultElementInfo]

type DefaultInternalDBObjectSupport = InternalDBObjectSupport[*DefaultElementInfo, DefaultElementInfo]

////////////////////////////////////////////////////////////////////////////////

type InternalDBObject interface {
	DBObject

	GetLock(phase mmids.Phase) mmids.RunId
	TryLock(phase mmids.Phase, id mmids.RunId) bool
	ClearLock(phase mmids.Phase, id mmids.RunId) bool

	GetStatus(phase mmids.Phase) model.Status
	SetStatus(phase mmids.Phase, status model.Status) bool
}

////////////////////////////////////////////////////////////////////////////////

type InternalObject interface {
	model.InternalObject
	GetBase() DBObject
}

type InternalObjectSupport struct { // cannot use struct type here (Go)
	Lock sync.Mutex
	Wrapper
}

// GetExternalState is a default implementation just forwarding
// the external state as provided by the external object.
func (n *InternalObjectSupport) GetExternalState(o model.ExternalObject, phase mmids.Phase) model.ExternalState {
	return o.GetState()
}

func (n *InternalObjectSupport) GetDatabase(ob objectbase.Objectbase) database.Database[DBObject] {
	return objectbase.GetDatabase[DBObject](ob)
}

func (n *InternalObjectSupport) GetDBObject() InternalDBObject {
	return utils.Cast[InternalDBObject](n.GetBase())
}

func (n *InternalObjectSupport) GetStatus(phase mmids.Phase) model.Status {
	n.Lock.Lock()
	defer n.Lock.Unlock()
	return n.GetDBObject().GetStatus(phase)
}

func (n *InternalObjectSupport) SetStatus(ob objectbase.Objectbase, phase mmids.Phase, status model.Status) (bool, error) {
	n.Lock.Lock()
	defer n.Lock.Unlock()

	mod := func(o DBObject) (bool, bool) {
		b := utils.Cast[InternalDBObject](o).SetStatus(phase, status)
		return b, b
	}
	return wrapped.Modify(ob, n, mod)
}

func (n *InternalObjectSupport) GetLock(phase mmids.Phase) mmids.RunId {
	n.Lock.Lock()
	defer n.Lock.Unlock()
	return n.GetDBObject().GetLock(phase)
}

func (n *InternalObjectSupport) TryLock(ob objectbase.Objectbase, phase mmids.Phase, id mmids.RunId) (bool, error) {
	n.Lock.Lock()
	defer n.Lock.Unlock()

	mod := func(o DBObject) (bool, bool) {
		b := utils.Cast[InternalDBObject](o).TryLock(phase, id)
		return b, b
	}
	return wrapped.Modify(ob, n, mod)
}

func (n *InternalObjectSupport) Rollback(lctx model.Logging, ob objectbase.Objectbase, phase mmids.Phase, id mmids.RunId) (bool, error) {
	n.Lock.Lock()
	defer n.Lock.Unlock()

	mod := func(_o DBObject) (bool, bool) {
		o := utils.Cast[InternalDBObject](_o)
		b := o.ClearLock(phase, id)
		return b, b
	}
	return wrapped.Modify(ob, n, mod)
}

type Committer interface {
	Commit(lctx model.Logging, _o InternalDBObject, phase mmids.Phase, spec *model.CommitInfo)
}

type CommitFunc func(lctx model.Logging, o InternalDBObject, phase mmids.Phase, spec *model.CommitInfo)

func (f CommitFunc) Commit(lctx model.Logging, o InternalDBObject, phase mmids.Phase, spec *model.CommitInfo) {
	f(lctx, o, phase, spec)
}

func (n *InternalObjectSupport) Commit(lctx model.Logging, ob objectbase.Objectbase, phase mmids.Phase, id mmids.RunId, commit *model.CommitInfo, committer Committer) (bool, error) {
	n.Lock.Lock()
	defer n.Lock.Unlock()

	mod := func(_o DBObject) (bool, bool) {
		o := utils.Cast[InternalDBObject](_o)
		b := o.ClearLock(phase, id)
		if b && commit != nil {
			committer.Commit(lctx, o, phase, commit)
		}
		return b, b
	}
	return wrapped.Modify(ob, n, mod)
}
