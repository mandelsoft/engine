package support

import (
	"fmt"
	"sync"

	"github.com/mandelsoft/engine/pkg/database"
	"github.com/mandelsoft/engine/pkg/processing/metamodel/objectbase"
	"github.com/mandelsoft/engine/pkg/processing/mmids"
	"github.com/mandelsoft/engine/pkg/processing/model/support/db"
	"github.com/mandelsoft/engine/pkg/utils"
)

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

func (n *Namespace) GetDatabase(ob objectbase.Objectbase) database.Database[db.DBObject] {
	return objectbase.GetDatabase[db.DBObject](ob)
}

func (n *Namespace) GetLock() mmids.RunId {
	return utils.Cast[db.DBNamespace](n.GetBase()).GetRunLock()
}

func (n *Namespace) ClearLock(ob objectbase.Objectbase, id mmids.RunId) (bool, error) {
	n.Lock.Lock()
	defer n.Lock.Unlock()

	dbo := n.GetDatabase(ob)
	mod := func(o db.DBObject) (bool, bool) {
		ns := utils.Cast[db.DBNamespace](o)
		b := ns.GetRunLock()
		if b != id {
			return false, false
		}
		ns.SetRunLock("")
		return true, true
	}

	o := n.GetBase()
	r, err := database.Modify(dbo, &o, mod)
	n.SetBase(o)
	return r, err
}

func (n *Namespace) TryLock(ob objectbase.Objectbase, id mmids.RunId) (bool, error) {
	n.Lock.Lock()
	defer n.Lock.Unlock()

	dbo := n.GetDatabase(ob)
	mod := func(o db.DBObject) (bool, bool) {
		ns := utils.Cast[db.DBNamespace](o)
		b := ns.GetRunLock()
		if b != "" {
			return false, false
		}
		ns.SetRunLock(id)
		return true, true
	}

	o := n.GetBase()
	r, err := database.Modify(dbo, &o, mod)
	n.SetBase(o)
	return r, err
}
