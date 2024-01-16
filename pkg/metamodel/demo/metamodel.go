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
	metamodel.ExtSpec(TYPE_NODE, TYPE_NODE_STATE, PHASE_UPDATING, metamodel.Dep(TYPE_NODE_STATE, PHASE_UPDATING)),
}

var internalTypes = []metamodel.InternalTypeSpecification{
	metamodel.IntSpec(TYPE_NODE_STATE, PHASE_UPDATING),
}

type MetaModel interface {
	metamodel.MetaModel

	// additional methods.
}

type MetaModelBase struct {
}

func (m *MetaModelBase) GetSpecification() metamodel.MetamodelSpecification {
	return metamodel.MetamodelSpecification{
		NamespaceType: TYPE_NAMESPACE,
		ExternalTypes: slices.Clone(externalTypes),
		InternalTypes: slices.Clone(internalTypes),
	}
}
