package demo

import (
	"slices"

	"github.com/mandelsoft/engine/pkg/metamodel"
)

const TYPE_NODE = "node"
const TYPE_NODE_STATE = "node-state"

var externalTypes = []string{TYPE_NODE}
var internalTypes = []string{TYPE_NODE_STATE}

type MetaModel struct {
}

var _ metamodel.MetaModel = (*MetaModel)(nil)

func (m *MetaModel) GetTypes() (external []string, internal []string) {
	return slices.Clone(externalTypes), slices.Clone((internalTypes))
}

func (m *MetaModel) GetProcessors() map[string]metamodel.Processor {
	// TODO implement me
	panic("implement me")
}

func (m *MetaModel) ProcessState(o) metamodel.Status
