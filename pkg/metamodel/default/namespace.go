package _default

import (
	"github.com/mandelsoft/engine/pkg/database"
	"github.com/mandelsoft/engine/pkg/metamodel"
)

type Namespace struct {
	database.GenerationObjectMeta

	RunLock metamodel.RunId `json:"runLock"`
}

var _ metamodel.Object = (*Namespace)(nil)

func (n *Namespace) Process(req metamodel.Request) metamodel.Status {
	return metamodel.Status{}
}

func (n *Namespace) TryLock(db database.Database, id metamodel.RunId) (bool, error) {
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
