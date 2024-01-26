package demo

import (
	"fmt"

	"github.com/mandelsoft/engine/pkg/impl/metamodels/demo/db"
	"github.com/mandelsoft/engine/pkg/metamodel/model"
	"github.com/mandelsoft/engine/pkg/metamodel/model/support"
	"github.com/mandelsoft/engine/pkg/metamodel/objectbase"
	"github.com/mandelsoft/engine/pkg/metamodel/objectbase/wrapped"
	"github.com/mandelsoft/engine/pkg/utils"
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

func (n *Node) UpdateStatus(ob objectbase.Objectbase, elem model.ElementId, update model.StatusUpdate) error {
	_, err := wrapped.Modify(ob, n, func(_o support.DBObject) (bool, bool) {
		o := _o.(*db.Node)
		mod := false
		support.UpdateField(&o.Status.RunId, update.RunId, &mod)
		support.UpdateField(&o.Status.EffectiveVersion, update.EffectiveVersion, &mod)
		if update.ObservedVersion != nil {
			fmt.Printf("\nUpdate observed version for Node %s to %s\n", n.GetName(), *update.ObservedVersion)
		}
		support.UpdateField(&o.Status.ObservedVersion, update.ObservedVersion, &mod)
		if update.DetectedVersion != nil {
			fmt.Printf("\nUpdate detected version for Node %s to %s\n", n.GetName(), *update.DetectedVersion)
		}
		support.UpdateField(&o.Status.DetectedVersion, update.DetectedVersion, &mod)
		support.UpdateField(&o.Status.State, update.Status, &mod)
		support.UpdateField(&o.Status.Message, update.Message, &mod)
		if update.ResultState != nil {
			support.UpdatePointerField(&o.Status.Result, utils.Pointer(update.ResultState.(*db.ResultState).GetState()), &mod)
		}
		return mod, mod
	})
	return err
}

type ExternalNodeState = support.ExternalState[*db.NodeSpec]
