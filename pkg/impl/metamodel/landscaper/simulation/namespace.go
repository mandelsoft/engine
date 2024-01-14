package simulation

import (
	"github.com/mandelsoft/engine/pkg/metamodel"
	"github.com/mandelsoft/engine/pkg/metamodel/landscaper"
)

type Namespace struct {
	Object
	Phase string
	Owner metamodel.RunId
}

var _ landscaper.Namespace = (*Namespace)(nil)

func NewNamespace(ns, name string) *Namespace {
	return &Namespace{
		Object: NewObject(landscaper.TYPE_NAMESPACE, ns, name),
		Phase:  landscaper.NS_PHASE_READY,
	}
}

func (n *Namespace) SetPhaseLocking(id metamodel.RunId) (bool, error) {
	n.lock.Lock()
	defer n.lock.Unlock()

	if n.Phase != landscaper.NS_PHASE_READY {
		return false, nil
	}
	n.Phase = landscaper.NS_PHASE_LOCKING
	n.Owner = id
	return true, nil
}

func (n *Namespace) SetPhaseReady() error {
	n.lock.Lock()
	defer n.lock.Unlock()
	n.Phase = landscaper.NS_PHASE_READY
	n.Owner = ""
	return nil
}
