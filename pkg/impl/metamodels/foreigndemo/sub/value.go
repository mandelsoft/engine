package sub

import (
	mymetamodel "github.com/mandelsoft/engine/pkg/metamodels/foreigndemo"
	. "github.com/mandelsoft/engine/pkg/processing/mmids"
	db2 "github.com/mandelsoft/engine/pkg/processing/model/support/db"
	"github.com/mandelsoft/engine/pkg/processing/objectbase"
	"github.com/mandelsoft/engine/pkg/processing/objectbase/wrapped"
	"github.com/mandelsoft/goutils/generics"

	"github.com/mandelsoft/engine/pkg/impl/metamodels/foreigndemo/sub/db"
	"github.com/mandelsoft/engine/pkg/processing/model"
	"github.com/mandelsoft/engine/pkg/processing/model/support"
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
	_, err := wrapped.Modify(ob, n, func(_o db2.Object) (bool, bool) {
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
		if update.FormalVersion != nil {
			log.Debug("Update formal version for Value {{name}} to {{state}}", "state", *update.FormalVersion)
		}
		support.UpdateField(&o.Status.FormalVersion, update.FormalVersion, &mod)
		support.UpdateField(&o.Status.Status, update.Status, &mod)
		support.UpdateField(&o.Status.Message, update.Message, &mod)
		if update.ResultState != nil {
			r := update.ResultState.(*ValueOutputState).GetState()
			// as slave object, result is stored in spec and not status
			if support.UpdateField(&o.Spec.Value, &r.Value, &mod) {
				log.Debug("update value for Value {{name}} to {{value}}", "value", r.Value)
			}

			provider := ""
			if r.Origin.GetType() != mymetamodel.TYPE_VALUE_STATE {
				provider = r.Origin.GetName()
			}
			if support.UpdateField(&o.Status.Provider, generics.Pointer(provider), &mod) {
				log.Debug("update provider for Value {{name}} to {{provider}}", "provider", provider)
			}
		}
		return mod, mod
	})
	return err
}

type ExternalValueState = support.ExternalState[*db.ValueSpec]
type EffectiveValueState = support.ExternalState[*db.EffectiveValueSpec]

var NewExternalValueState = support.NewExternalState[*db.EffectiveValueSpec]
var NewEffectiveValueState = support.NewExternalState[*db.EffectiveValueSpec]
