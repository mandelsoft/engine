package support

import (
	"fmt"
	"sync"

	"github.com/mandelsoft/engine/pkg/database"
	"github.com/mandelsoft/engine/pkg/metamodel/model/common"
	"github.com/mandelsoft/engine/pkg/metamodel/model/objectbase"
	"github.com/mandelsoft/engine/pkg/utils"
)

type DBNamespace interface {
	DBObject
	GetRunLock() common.RunId
	SetRunLock(id common.RunId)
}

type Namespace struct {
	Lock sync.Mutex
	Wrapper
}

var _ common.Object = (*Namespace)(nil)

func (n *Namespace) GetNamespaceName() string {
	if n.GetNamespace() == "" {
		return n.GetName()
	}
	return fmt.Sprintf("%s/%s", n.GetNamespace(), n.GetName())
}

func (n *Namespace) GetDatabase(ob objectbase.Objectbase) database.Database[DBObject] {
	return objectbase.GetDatabase[DBObject](ob)
}

func (n *Namespace) GetLock() common.RunId {
	return utils.Cast[DBNamespace](n.GetBase()).GetRunLock()
}

func (n *Namespace) TryLock(ob objectbase.Objectbase, id common.RunId) (bool, error) {
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
