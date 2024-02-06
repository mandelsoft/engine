package support

import (
	"fmt"
	"sync"

	"github.com/mandelsoft/engine/pkg/database"
	"github.com/mandelsoft/engine/pkg/processing/metamodel/objectbase"
	"github.com/mandelsoft/engine/pkg/processing/mmids"
	"github.com/mandelsoft/engine/pkg/utils"
)

type DBNamespace interface {
	DBObject
	GetRunLock() mmids.RunId
	SetRunLock(id mmids.RunId)
}

type Namespace struct {
	Lock sync.Mutex
	Wrapper
}

var _ objectbase.Object = (*Namespace)(nil)

func (n *Namespace) GetNamespaceName() string {
	if n.GetNamespace() == "" {
		return n.GetName()
	}
	return fmt.Sprintf("%s/%s", n.GetNamespace(), n.GetName())
}

func (n *Namespace) GetDatabase(ob objectbase.Objectbase) database.Database[DBObject] {
	return objectbase.GetDatabase[DBObject](ob)
}

func (n *Namespace) GetLock() mmids.RunId {
	return utils.Cast[DBNamespace](n.GetBase()).GetRunLock()
}

func (n *Namespace) ClearLock(ob objectbase.Objectbase, id mmids.RunId) (bool, error) {
	n.Lock.Lock()
	defer n.Lock.Unlock()

	db := n.GetDatabase(ob)
	mod := func(o DBObject) (bool, bool) {
		ns := utils.Cast[DBNamespace](o)
		b := ns.GetRunLock()
		if b != id {
			return false, false
		}
		ns.SetRunLock("")
		return true, true
	}

	o := n.GetBase()
	r, err := database.Modify(db, &o, mod)
	n.SetBase(o)
	return r, err
}

func (n *Namespace) TryLock(ob objectbase.Objectbase, id mmids.RunId) (bool, error) {
	n.Lock.Lock()
	defer n.Lock.Unlock()

	db := n.GetDatabase(ob)
	mod := func(o DBObject) (bool, bool) {
		ns := utils.Cast[DBNamespace](o)
		b := ns.GetRunLock()
		if b != "" {
			return false, false
		}
		ns.SetRunLock(id)
		return true, true
	}

	o := n.GetBase()
	r, err := database.Modify(db, &o, mod)
	n.SetBase(o)
	return r, err
}