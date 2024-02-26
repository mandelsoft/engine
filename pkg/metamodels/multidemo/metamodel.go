package multidemo

import (
	"slices"

	"github.com/mandelsoft/engine/pkg/processing/metamodel"
)

const TYPE_NAMESPACE = "Namespace"

const TYPE_NODE = "Node"
const TYPE_NODE_STATE = "NodeState"

const PHASE_GATHER = "Gathering"
const PHASE_CALCULATION = "Calculating"

const FINAL_PHASE = PHASE_CALCULATION

var externalTypes = []metamodel.ExternalTypeSpecification{
	metamodel.ExtSpec(TYPE_NODE, TYPE_NODE_STATE, PHASE_GATHER),
}

var internalTypes = []metamodel.InternalTypeSpecification{
	metamodel.IntSpec(TYPE_NODE_STATE,
		metamodel.PhaseSpec(PHASE_GATHER, metamodel.Dep(TYPE_NODE_STATE, PHASE_CALCULATION)),
		metamodel.PhaseSpec(PHASE_CALCULATION, metamodel.LocalDep(PHASE_GATHER)),
	),
}

func MetaModelSpecification() metamodel.MetaModelSpecification {
	return metamodel.MetaModelSpecification{
		NamespaceType: TYPE_NAMESPACE,
		ExternalTypes: slices.Clone(externalTypes),
		InternalTypes: slices.Clone(internalTypes),
	}
}

func NewMetaModel(name string) (metamodel.MetaModel, error) {
	return metamodel.NewMetaModel(name, MetaModelSpecification())
}
