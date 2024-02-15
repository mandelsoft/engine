package delivery

import (
	. "github.com/mandelsoft/engine/pkg/processing/mmids"
	db2 "github.com/mandelsoft/engine/pkg/processing/model/support/db"

	"github.com/mandelsoft/engine/pkg/processing/metamodel/objectbase"
	"github.com/mandelsoft/engine/pkg/processing/metamodel/objectbase/wrapped"
	"github.com/mandelsoft/engine/pkg/processing/model"
	"github.com/mandelsoft/engine/pkg/processing/model/support"
	"github.com/mandelsoft/engine/pkg/utils"

	"github.com/mandelsoft/engine/pkg/impl/metamodels/valopdemo/delivery/db"
	mymetamodel "github.com/mandelsoft/engine/pkg/metamodels/valopdemo"
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

func (n *Value) UpdateStatus(lctx model.Logging, ob objectbase.Objectbase, elem ElementId, update model.StatusUpdate) error {
	log := lctx.Logger(REALM).WithValues("name", n.GetName())
	_, err := wrapped.Modify(ob, n, func(_o db2.DBObject) (bool, bool) {
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
			r := update.ResultState.(*ValueOutputState).GetState()
			// as slave object, result is stored in spec and not status
			log.Debug("Update value for Value {{name}} to {{value}}", "value", r.Value)
			support.UpdateField(&o.Spec.Value, &r.Value, &mod)

			provider := ""
			if r.Origin.GetType() != mymetamodel.TYPE_VALUE_STATE {
				provider = r.Origin.GetName()
			}
			log.Debug("Update provider for Value {{name}} to {{provider}}", "provider", provider)
			support.UpdateField(&o.Status.Provider, utils.Pointer(provider), &mod)
		}
		return mod, mod
	})
	return err
}

type ExternalValueState = support.ExternalState[*db.ValueSpec]
type EffectiveValueState = support.ExternalState[*db.EffectiveValueSpec]

var NewExternalValueState = support.NewExternalState[*db.EffectiveValueSpec]
var NewEffectiveValueState = support.NewExternalState[*db.EffectiveValueSpec]
