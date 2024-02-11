package foreigndemo

import (
	. "github.com/mandelsoft/engine/pkg/processing/mmids"

	"github.com/mandelsoft/engine/pkg/impl/metamodels/foreigndemo/db"
	"github.com/mandelsoft/engine/pkg/processing/metamodel/objectbase"
	"github.com/mandelsoft/engine/pkg/processing/metamodel/objectbase/wrapped"
	"github.com/mandelsoft/engine/pkg/processing/model"
	"github.com/mandelsoft/engine/pkg/processing/model/support"
)

func init() {
	wrapped.MustRegisterType[Expression](scheme)
}

type Expression struct {
	support.ExternalObjectSupport
}

var _ model.ExternalObject = (*Expression)(nil)

func (n *Expression) GetState() model.ExternalState {
	return support.NewExternalState[*db.OperatorSpec](&n.GetBase().(*db.Operator).Spec)
}

func (n *Expression) UpdateStatus(lctx model.Logging, ob objectbase.Objectbase, elem ElementId, update model.StatusUpdate) error {
	// no status update from phase so far
	return nil
}

type ExternalExpressionState = support.ExternalState[db.ExpressionOutput]
