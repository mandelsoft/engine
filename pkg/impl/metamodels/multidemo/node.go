package multidemo

import (
	. "github.com/mandelsoft/engine/pkg/processing/mmids"
	db2 "github.com/mandelsoft/engine/pkg/processing/model/support/db"
	"github.com/mandelsoft/engine/pkg/processing/objectbase"
	wrapped2 "github.com/mandelsoft/engine/pkg/processing/objectbase/wrapped"
	"github.com/mandelsoft/goutils/generics"

	"github.com/mandelsoft/engine/pkg/impl/metamodels/multidemo/db"
	mymetamodel "github.com/mandelsoft/engine/pkg/metamodels/multidemo"
	"github.com/mandelsoft/engine/pkg/processing/model"
	"github.com/mandelsoft/engine/pkg/processing/model/support"
)

func init() {
	wrapped2.MustRegisterType[Node](scheme)
}

type Node struct {
	support.ExternalObjectSupport
}

var _ model.ExternalObject = (*Node)(nil)

func (n *Node) GetState() model.ExternalState {
	return support.NewExternalState[*db.NodeSpec](&n.GetBase().(*db.Node).Spec)
}

func (n *Node) UpdateStatus(lctx model.Logging, ob objectbase.Objectbase, elem ElementId, update model.StatusUpdate) error {
	log := lctx.Logger(REALM).WithValues("name", n.GetName())
	_, err := wrapped2.Modify(ob, n, func(_o db2.Object) (bool, bool) {
		o := _o.(*db.Node)
		mod := false
		support.UpdateField(&o.Status.Phase, generics.Pointer(elem.GetPhase()), &mod)
		support.UpdateField(&o.Status.RunId, update.RunId, &mod)
		support.UpdateField(&o.Status.DetectedVersion, update.DetectedVersion, &mod)
		if elem.GetPhase() == mymetamodel.PHASE_CALCULATION {
			support.UpdateField(&o.Status.EffectiveVersion, update.EffectiveVersion, &mod)
			if update.ObservedVersion != nil {
				log.Debug("Update observed version for Node {{name}} to {{state}}", "state", *update.ObservedVersion)
			}
			support.UpdateField(&o.Status.ObservedVersion, update.ObservedVersion, &mod)
			if update.DetectedVersion != nil {
				log.Debug("Update detected version for Node {{name}}}} to {{state}}", "state", *update.DetectedVersion)
			}
			if update.ResultState != nil {
				support.UpdatePointerField(&o.Status.Result, generics.Pointer(update.ResultState.(*CalcOutputState).GetState()), &mod)
			}
		} else {
			support.UpdateField(&o.Status.Status, update.Status, &mod)
		}
		support.UpdateField(&o.Status.Message, update.Message, &mod)
		return mod, mod
	})
	return err
}

type ExternalNodeState = support.ExternalState[*db.NodeSpec]
