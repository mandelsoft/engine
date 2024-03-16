package sub

import (
	. "github.com/mandelsoft/engine/pkg/processing/mmids"

	"github.com/mandelsoft/engine/pkg/impl/metamodels/foreigndemo/sub/db"
	"github.com/mandelsoft/engine/pkg/processing/model"
	"github.com/mandelsoft/engine/pkg/processing/model/support"
	db2 "github.com/mandelsoft/engine/pkg/processing/model/support/db"
	"github.com/mandelsoft/engine/pkg/processing/objectbase"
	"github.com/mandelsoft/engine/pkg/processing/objectbase/wrapped"
)

func init() {
	wrapped.MustRegisterType[Expression](scheme)
}

type Expression struct {
	support.ExternalObjectSupport
}

var _ model.ExternalObject = (*Expression)(nil)

func (n *Expression) GetState() model.ExternalState {
	// The complete state of the expression object is used
	// to reflect changes in spec as well as output and status.
	dbo := n.GetDBObject().(*db.Expression)
	return NewExternalExpressionState(db.NewExternalExpressionSpec(dbo))
}

func (n *Expression) UpdateStatus(lctx model.Logging, ob objectbase.Objectbase, elem ElementId, update model.StatusUpdate) error {
	log := lctx.Logger(REALM).WithValues("name", n.GetName())
	_, err := wrapped.Modify(ob, n, func(_o db2.Object) (bool, bool) {
		o := _o.(*db.Expression)
		mod := false

		if update.ExternalState != nil {
			r := update.ExternalState.(*EffectiveExpressionState).GetState()
			provider := r.Provider
			log.Debug("Update provider for Expression {{name}} to {{provider}}", "provider", provider)
			support.UpdateField(&o.Status.Provider, &provider, &mod)
		}
		return mod, mod
	})
	return err
}

type ExternalExpressionState = support.ExternalState[*db.ExternalExpressionSpec]
type EffectiveExpressionState = support.ExternalState[*db.EffectiveExpressionSpec]

var NewExternalExpressionState = support.NewExternalState[*db.ExternalExpressionSpec]
var NewEffectiveExpressionState = support.NewExternalState[*db.EffectiveExpressionSpec]
