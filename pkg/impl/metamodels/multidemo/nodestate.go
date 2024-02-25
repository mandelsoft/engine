package multidemo

import (
	"github.com/mandelsoft/engine/pkg/processing/model"
	"github.com/mandelsoft/engine/pkg/processing/model/support"
	"github.com/mandelsoft/engine/pkg/processing/objectbase/wrapped"
	"github.com/mandelsoft/engine/pkg/runtime"

	"github.com/mandelsoft/engine/pkg/impl/metamodels/multidemo/db"
	mymetamodel "github.com/mandelsoft/engine/pkg/metamodels/multidemo"
)

func init() {
	wrapped.MustRegisterType[NodeState](scheme)
}

type NodeState struct {
	support.InternalPhaseObjectSupport[*NodeState, *db.NodeState]
}

var _ runtime.InitializedObject = (*NodeState)(nil)

func (n *NodeState) Initialize() error {
	return support.SetSelf(n, nodeStatePhases, db.NodePhaseStateAccess)
}

var _ model.InternalObject = (*NodeState)(nil)

var nodeStatePhases = support.NewPhases[*NodeState, *db.NodeState](REALM)

func init() {
	nodeStatePhases.Register(mymetamodel.PHASE_GATHER, GatherPhase{})
	nodeStatePhases.Register(mymetamodel.PHASE_CALCULATION, CalculatePhase{})
}

type NodeStatePhase = support.Phase[*NodeState, *db.NodeState]

////////////////////////////////////////////////////////////////////////////////

type PhaseBase struct {
	support.DefaultPhase[*NodeState, *db.NodeState]
}
