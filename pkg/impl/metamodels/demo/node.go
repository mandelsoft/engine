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
		support.UpdateField(&o.Status.ObservedVersion, update.ObservedVersion, &mod)
		support.UpdateField(&o.Status.DetectedVersion, update.DetectedVersion, &mod)
		support.UpdateField(&o.Status.State, update.Status, &mod)
		support.UpdateField(&o.Status.Message, update.Message, &mod)
		if update.InternalState != nil {
			support.UpdatePointerField(&o.Status.Result, utils.Pointer(update.InternalState.(InternalState).output), &mod)
		}
		return mod, mod
	})
	return err
}

type ExternalNodeState = support.ExternalState[*db.NodeSpec]

type InternalState struct {
	output int
}

var _ model.InternalState = InternalState{}

func (i InternalState) Description() string {
	return fmt.Sprintf("%d", i.output)
}
