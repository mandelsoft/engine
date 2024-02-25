package sub

import (
	"github.com/mandelsoft/engine/pkg/processing/model"
	"github.com/mandelsoft/engine/pkg/processing/model/support"
	"github.com/mandelsoft/engine/pkg/processing/objectbase/wrapped"
	"github.com/mandelsoft/engine/pkg/runtime"

	"github.com/mandelsoft/engine/pkg/impl/metamodels/foreigndemo/sub/db"
	mymetamodel "github.com/mandelsoft/engine/pkg/metamodels/foreigndemo"
)

func init() {
	wrapped.MustRegisterType[OperatorState](scheme)
}

type OperatorState struct {
	support.InternalPhaseObjectSupport[*OperatorState, *db.OperatorState]
}

var _ runtime.InitializedObject = (*OperatorState)(nil)

func (n *OperatorState) Initialize() error {
	return support.SetSelf(n, nodeStatePhases, db.OperatorPhaseStateAccess)
}

var _ model.InternalObject = (*OperatorState)(nil)

var nodeStatePhases = support.NewPhases[*OperatorState, *db.OperatorState](REALM)

func init() {
	nodeStatePhases.Register(mymetamodel.PHASE_GATHER, GatherPhase{})
	nodeStatePhases.Register(mymetamodel.PHASE_CALCULATION, CalculatePhase{})
}

type OperatorStatePhase = support.Phase[*OperatorState, *db.OperatorState]

////////////////////////////////////////////////////////////////////////////////

type PhaseBase struct {
	support.DefaultPhase[*OperatorState, *db.OperatorState]
}
