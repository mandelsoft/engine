package demo

import (
	"slices"

	metamodel2 "github.com/mandelsoft/engine/pkg/processing/metamodel"
)

const TYPE_NAMESPACE = "Namespace"

const TYPE_NODE = "Node"
const TYPE_NODE_STATE = "NodeState"

const PHASE_UPDATING = "Updating"

const FINAL_PHASE = PHASE_UPDATING

var externalTypes = []metamodel2.ExternalTypeSpecification{
	metamodel2.ExtSpec(TYPE_NODE, TYPE_NODE_STATE, PHASE_UPDATING),
}

var internalTypes = []metamodel2.InternalTypeSpecification{
	metamodel2.IntSpec(TYPE_NODE_STATE,
		metamodel2.PhaseSpec(PHASE_UPDATING, metamodel2.Dep(TYPE_NODE_STATE, PHASE_UPDATING))),
}

func MetaModelSpecification() metamodel2.MetaModelSpecification {
	return metamodel2.MetaModelSpecification{
		NamespaceType: TYPE_NAMESPACE,
		ExternalTypes: slices.Clone(externalTypes),
		InternalTypes: slices.Clone(internalTypes),
	}
}

func NewMetaModel(name string) (metamodel2.MetaModel, error) {
	return metamodel2.NewMetaModel(name, MetaModelSpecification())
}
