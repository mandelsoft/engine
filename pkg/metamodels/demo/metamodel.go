package demo

import (
	"slices"

	"github.com/mandelsoft/engine/pkg/metamodel"
)

const TYPE_NAMESPACE = "Namespace"

const TYPE_NODE = "Node"
const TYPE_NODE_STATE = "NodeState"

const PHASE_UPDATING = "Updating"

var externalTypes = []metamodel.ExternalTypeSpecification{
	metamodel.ExtSpec(TYPE_NODE, TYPE_NODE_STATE, PHASE_UPDATING),
}

var internalTypes = []metamodel.InternalTypeSpecification{
	metamodel.IntSpec(TYPE_NODE_STATE,
		metamodel.PhaseSpec(PHASE_UPDATING, metamodel.Dep(TYPE_NODE_STATE, PHASE_UPDATING))),
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
