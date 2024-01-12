package simulation

import (
	"github.com/mandelsoft/engine/pkg/database"
)

type Namespace struct {
	Object
	Phase string
	Owner database.RunId
}

var _ database.Namespace = (*Namespace)(nil)

func NewNamespace(name string) *Namespace {
	return &Namespace{
		Object: NewObject(database.TYPE_NAMESPACE, name),
		Phase:  database.NS_PHASE_READY,
	}
}

func (n *Namespace) SetPhaseLocking(id database.RunId) (bool, error) {
	n.lock.Lock()
	defer n.lock.Unlock()

	if n.Phase != database.NS_PHASE_READY {
		return false, nil
	}
	n.Phase = database.NS_PHASE_LOCKING
	n.Owner = id
	return true, nil
}

func (n *Namespace) SetPhaseReady() error {
	n.lock.Lock()
	defer n.lock.Unlock()
	n.Phase = database.NS_PHASE_READY
	n.Owner = ""
	return nil
}
