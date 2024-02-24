package sub

import (
	. "github.com/mandelsoft/engine/pkg/processing/mmids"

	"github.com/mandelsoft/engine/pkg/processing/model"
	"github.com/mandelsoft/engine/pkg/processing/model/support"
	"github.com/mandelsoft/engine/pkg/processing/objectbase"
	"github.com/mandelsoft/engine/pkg/processing/objectbase/wrapped"

	"github.com/mandelsoft/engine/pkg/impl/metamodels/foreigndemo/sub/db"
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
	return NewExternalExpressionState(db.NewEffectiveExpressionSpec(dbo))
}

func (n *Expression) UpdateStatus(lctx model.Logging, ob objectbase.Objectbase, elem ElementId, update model.StatusUpdate) error {
	// no status update from phase so far
	return nil
}

type ExternalExpressionState = support.ExternalState[*db.EffectiveExpressionSpec]

func NewExternalExpressionState(s *db.EffectiveExpressionSpec) *ExternalExpressionState {
	return support.NewExternalState(s)
}
