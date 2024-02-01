package valopdemo

import (
	"github.com/mandelsoft/engine/pkg/metamodel/common"
	"github.com/mandelsoft/engine/pkg/metamodel/model"
	"github.com/mandelsoft/engine/pkg/metamodel/model/support"
	"github.com/mandelsoft/engine/pkg/metamodel/objectbase"
	"github.com/mandelsoft/engine/pkg/metamodel/objectbase/wrapped"
	"github.com/mandelsoft/engine/pkg/utils"

	"github.com/mandelsoft/engine/pkg/impl/metamodels/valopdemo/db"
	mymetamodel "github.com/mandelsoft/engine/pkg/metamodels/valopdemo"
)

func init() {
	wrapped.MustRegisterType[Operator](scheme)
}

type Operator struct {
	support.ExternalObjectSupport
}

var _ model.ExternalObject = (*Operator)(nil)

func (n *Operator) GetState() model.ExternalState {
	return support.NewExternalState[*db.OperatorSpec](&n.GetBase().(*db.Operator).Spec)
}

func (n *Operator) UpdateStatus(lctx common.Logging, ob objectbase.Objectbase, elem model.ElementId, update model.StatusUpdate) error {
	log := lctx.Logger(db.REALM).WithValues("name", n.GetName())
	_, err := wrapped.Modify(ob, n, func(_o support.DBObject) (bool, bool) {
		o := _o.(*db.Operator)
		mod := false
		support.UpdateField(&o.Status.Phase, utils.Pointer(elem.Phase()), &mod)
		support.UpdateField(&o.Status.RunId, update.RunId, &mod)
		support.UpdateField(&o.Status.DetectedVersion, update.DetectedVersion, &mod)
		if elem.Phase() == mymetamodel.PHASE_CALCULATION {
			support.UpdateField(&o.Status.EffectiveVersion, update.EffectiveVersion, &mod)
			if update.ObservedVersion != nil {
				log.Debug("Update observed version for Node {{name}} to {{state}}", "state", *update.ObservedVersion)
			}
			support.UpdateField(&o.Status.ObservedVersion, update.ObservedVersion, &mod)
			if update.DetectedVersion != nil {
				log.Debug("Update detected version for Node {{name}}}} to {{state}}", "state", *update.DetectedVersion)
			}
			if update.ResultState != nil {
				support.UpdatePointerField(&o.Status.Result, utils.Pointer(update.ResultState.(*db.CalcResultState).GetState()), &mod)
			}
		} else {
			support.UpdateField(&o.Status.Status, update.Status, &mod)
		}
		support.UpdateField(&o.Status.Message, update.Message, &mod)
		return mod, mod
	})
	return err
}

type ExternalOperatorState = support.ExternalState[*db.OperatorSpec]
