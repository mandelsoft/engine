package valopdemo

import (
	"slices"

	metamodel2 "github.com/mandelsoft/engine/pkg/processing/metamodel"
)

const TYPE_NAMESPACE = "Namespace"

const TYPE_VALUE = "Value"
const TYPE_VALUE_STATE = "ValueState"

const PHASE_PROPAGATE = "Propagating"

const TYPE_OPERATOR = "Operator"
const TYPE_OPERATOR_STATE = "OperatorState"

const PHASE_GATHER = "Gathering"
const PHASE_CALCULATION = "Calculating"

const FINAL_VALUE_PHASE = PHASE_PROPAGATE
const FINAL_OPERATOR_PHASE = PHASE_CALCULATION

var externalTypes = []metamodel2.ExternalTypeSpecification{
	metamodel2.ExtSpec(TYPE_VALUE, TYPE_VALUE_STATE, PHASE_PROPAGATE),
	metamodel2.ExtSpec(TYPE_OPERATOR, TYPE_OPERATOR_STATE, PHASE_GATHER),
}

var internalTypes = []metamodel2.InternalTypeSpecification{
	metamodel2.IntSpec(TYPE_VALUE_STATE,
		metamodel2.PhaseSpec(PHASE_PROPAGATE, metamodel2.Dep(TYPE_OPERATOR_STATE, PHASE_CALCULATION)),
	),
	metamodel2.IntSpec(TYPE_OPERATOR_STATE,
		metamodel2.PhaseSpec(PHASE_GATHER, metamodel2.Dep(TYPE_VALUE_STATE, PHASE_PROPAGATE)),
		metamodel2.PhaseSpec(PHASE_CALCULATION, metamodel2.Dep(TYPE_OPERATOR_STATE, PHASE_GATHER)),
	),
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
