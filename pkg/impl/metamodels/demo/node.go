package demo

import (
	"github.com/mandelsoft/engine/pkg/metamodel/common"
	"github.com/mandelsoft/engine/pkg/metamodel/model"
	"github.com/mandelsoft/engine/pkg/metamodel/model/support"
	"github.com/mandelsoft/engine/pkg/metamodel/objectbase"
	"github.com/mandelsoft/engine/pkg/metamodel/objectbase/wrapped"
	"github.com/mandelsoft/engine/pkg/utils"

	"github.com/mandelsoft/engine/pkg/impl/metamodels/demo/db"
)

func init() {
	wrapped.MustRegisterType[Node](scheme)
}

type Node struct {
	support.ExternalObjectSupport
}

var _ model.ExternalObject = (*Node)(nil)

func (n *Node) GetState() model.ExternalState {
	return support.NewExternalState[*db.NodeSpec](&n.GetBase().(*db.Node).Spec)
}

func (n *Node) UpdateStatus(lctx common.Logging, ob objectbase.Objectbase, elem model.ElementId, update model.StatusUpdate) error {
	log := lctx.Logger(REALM).WithValues("name", n.GetName())
	_, err := wrapped.Modify(ob, n, func(_o support.DBObject) (bool, bool) {
		o := _o.(*db.Node)
		mod := false
		support.UpdateField(&o.Status.RunId, update.RunId, &mod)
		support.UpdateField(&o.Status.EffectiveVersion, update.EffectiveVersion, &mod)
		if update.ObservedVersion != nil {
			log.Debug("Update observed version for Node {{name}} to {{state}}", "state", *update.ObservedVersion)
		}
		support.UpdateField(&o.Status.ObservedVersion, update.ObservedVersion, &mod)
		if update.DetectedVersion != nil {
			log.Debug("Update detected version for Node {{name}} to {{state}}", "state", *update.DetectedVersion)
		}
		support.UpdateField(&o.Status.DetectedVersion, update.DetectedVersion, &mod)
		support.UpdateField(&o.Status.Status, update.Status, &mod)
		support.UpdateField(&o.Status.Message, update.Message, &mod)
		if update.ResultState != nil {
			support.UpdatePointerField(&o.Status.Result, utils.Pointer(update.ResultState.(*ResultState).GetState()), &mod)
		}
		return mod, mod
	})
	return err
}

type ExternalNodeState = support.ExternalState[*db.NodeSpec]
