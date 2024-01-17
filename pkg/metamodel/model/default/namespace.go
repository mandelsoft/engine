package _default

import (
	"github.com/mandelsoft/engine/pkg/database"
	"github.com/mandelsoft/engine/pkg/metamodel/model/common"
)

type Namespace struct {
	database.GenerationObjectMeta

	RunLock common.RunId `json:"runLock"`
}

var _ common.Object = (*Namespace)(nil)

func (n *Namespace) Process(req common.Request) common.Status {
	return common.Status{}
}

func (n *Namespace) TryLock(db database.Database, id common.RunId) (bool, error) {
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
