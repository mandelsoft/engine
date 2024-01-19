package _default

import (
	"fmt"

	"github.com/mandelsoft/engine/pkg/database"
	"github.com/mandelsoft/engine/pkg/metamodel/model/common"
	"github.com/mandelsoft/engine/pkg/metamodel/model/objectbase"
)

type Namespace struct {
	database.GenerationObjectMeta

	RunLock common.RunId `json:"runLock"`
}

var _ common.Object = (*Namespace)(nil)

func (n *Namespace) GetNamespaceName() string {
	if n.GetNamespace() == "" {
		return n.GetName()
	}
	return fmt.Sprintf("%s/%s", n.GetNamespace(), n.GetName())
}

func (n *Namespace) GetLock() common.RunId {
	return n.RunLock
}

func (n *Namespace) TryLock(db objectbase.Objectbase, id common.RunId) (bool, error) {
	var on *Namespace

	err := database.ErrModified
	for {
		var o database.Object

		o, err = db.GetObject(n)
		if err != nil {
			return false, err
		}
		on = o.(*Namespace)
		if on.RunLock != "" {
			return false, nil
		}
		on.RunLock = id
		err = db.SetObject(n)
		if err != database.ErrModified {
			return false, err
		}
	}
	*n = *on
	return true, nil
}
