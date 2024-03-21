package foreigndemo

import (
	"slices"

	"github.com/mandelsoft/engine/pkg/processing/metamodel"
)

const TYPE_NAMESPACE = "Namespace"
const TYPE_UPDATEREQUEST = "UpdateRequest" // not available for all implementations

const TYPE_VALUE = "Value"
const TYPE_VALUE_STATE = "ValueState"

const PHASE_PROPAGATE = "Propagating"

const TYPE_OPERATOR = "Operator"
const TYPE_OPERATOR_STATE = "OperatorState"

const PHASE_GATHER = "Gathering"
const PHASE_EXPOSE = "Exposing"

const TYPE_EXPRESSION = "Expression"
const TYPE_EXPRESSION_STATE = "ExpressionState"

const PHASE_CALCULATE = "Calculating"

const FINAL_VALUE_PHASE = PHASE_PROPAGATE
const FINAL_OPERATOR_PHASE = PHASE_EXPOSE
const FINAL_EXPRESSION_PHASE = PHASE_CALCULATE

var externalTypes = []metamodel.ExternalTypeSpecification{
	metamodel.ExtSpec(TYPE_VALUE, TYPE_VALUE_STATE, PHASE_PROPAGATE),
	metamodel.ExtSpec(TYPE_OPERATOR, TYPE_OPERATOR_STATE, PHASE_GATHER),
	metamodel.ExtSpec(TYPE_EXPRESSION, TYPE_EXPRESSION_STATE, PHASE_CALCULATE).
		Foreign(),
}

var internalTypes = []metamodel.InternalTypeSpecification{
	metamodel.IntSpec(TYPE_VALUE_STATE,
		metamodel.PhaseSpec(PHASE_PROPAGATE, metamodel.Dep(TYPE_OPERATOR_STATE, PHASE_EXPOSE)),
	),
	metamodel.IntSpec(TYPE_EXPRESSION_STATE,
		metamodel.PhaseSpec(PHASE_CALCULATE, metamodel.Dep(TYPE_OPERATOR_STATE, PHASE_GATHER)),
	),
	metamodel.IntSpec(TYPE_OPERATOR_STATE,
		metamodel.PhaseSpec(PHASE_GATHER, metamodel.Dep(TYPE_VALUE_STATE, PHASE_PROPAGATE)),
		metamodel.PhaseSpec(PHASE_EXPOSE,
			metamodel.LocalDep(PHASE_GATHER),
			metamodel.Dep(TYPE_EXPRESSION_STATE, PHASE_CALCULATE)),
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

func MustMetaModel(name string) metamodel.MetaModel {
	mm, err := metamodel.NewMetaModel(name, MetaModelSpecification())
	if err != nil {
		panic(err)
	}
	return mm
}
