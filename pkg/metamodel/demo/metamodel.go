package demo

import (
	"slices"

	"github.com/mandelsoft/engine/pkg/metamodel"
)

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

func (m *MetaModelBase) GetTypes() (external []metamodel.ExternalTypeSpecification, internal []metamodel.InternalTypeSpecification) {
	return slices.Clone(externalTypes), slices.Clone(internalTypes)
}
