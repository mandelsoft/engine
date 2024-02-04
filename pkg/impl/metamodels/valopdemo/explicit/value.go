package explicit

import (
	"github.com/mandelsoft/engine/pkg/impl/metamodels/valopdemo/explicit/db"
	"github.com/mandelsoft/engine/pkg/metamodel/common"
	"github.com/mandelsoft/engine/pkg/metamodel/model"
	"github.com/mandelsoft/engine/pkg/metamodel/model/support"
	"github.com/mandelsoft/engine/pkg/metamodel/objectbase"
	"github.com/mandelsoft/engine/pkg/metamodel/objectbase/wrapped"
	"github.com/mandelsoft/engine/pkg/utils"
)

func init() {
	wrapped.MustRegisterType[Value](scheme)
}

type Value struct {
	support.ExternalObjectSupport
}

var _ model.ExternalObject = (*Value)(nil)

func (n *Value) GetState() model.ExternalState {
	return support.NewExternalState[*db.ValueSpec](&n.GetBase().(*db.Value).Spec)
}

func (n *Value) UpdateStatus(lctx common.Logging, ob objectbase.Objectbase, elem model.ElementId, update model.StatusUpdate) error {
	log := lctx.Logger(REALM).WithValues("name", n.GetName())
	_, err := wrapped.Modify(ob, n, func(_o support.DBObject) (bool, bool) {
		o := _o.(*db.Value)
		mod := false
		support.UpdateField(&o.Status.RunId, update.RunId, &mod)
		support.UpdateField(&o.Status.EffectiveVersion, update.EffectiveVersion, &mod)
		if update.ObservedVersion != nil {
			log.Debug("Update observed version for Value {{name}} to {{state}}", "state", *update.ObservedVersion)
		}
		support.UpdateField(&o.Status.ObservedVersion, update.ObservedVersion, &mod)
		if update.DetectedVersion != nil {
			log.Debug("Update detected version for Value {{name}} to {{state}}", "state", *update.DetectedVersion)
		}
		support.UpdateField(&o.Status.DetectedVersion, update.DetectedVersion, &mod)
		support.UpdateField(&o.Status.Status, update.Status, &mod)
		support.UpdateField(&o.Status.Message, update.Message, &mod)
		if update.ResultState != nil {
			support.UpdatePointerField(&o.Status.Result, utils.Pointer(update.ResultState.(*ValueOutputState).GetState().Value), &mod)
		}
		return mod, mod
	})
	return err
}

type ExternalValueState = support.ExternalState[*db.ValueSpec]
