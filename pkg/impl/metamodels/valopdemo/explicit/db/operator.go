package db

import (
	"slices"

	"github.com/mandelsoft/engine/pkg/database"
	"github.com/mandelsoft/engine/pkg/metamodel/common"
	"github.com/mandelsoft/engine/pkg/metamodel/model"
	"github.com/mandelsoft/engine/pkg/metamodel/model/support"

	mymetamodel "github.com/mandelsoft/engine/pkg/metamodels/valopdemo"
)

func init() {
	database.MustRegisterType[Operator, support.DBObject](Scheme) // Goland requires second type parameter
}

type Operator struct {
	database.GenerationObjectMeta

	Spec   OperatorSpec   `json:"spec"`
	Status OperatorStatus `json:"status"`
}

var _ database.Object = (*Operator)(nil)

type OperatorName string

const OP_ADD = OperatorName("add")
const OP_SUB = OperatorName("sub")
const OP_MUL = OperatorName("mul")
const OP_DIV = OperatorName("div")

type OperatorSpec struct {
	Operator OperatorName `json:"operator"`
	Operands []string     `json:"operands,omitempty"`
}

type OperatorStatus struct {
	Phase            model.Phase             `json:"phase,omitempty"`
	Status           common.ProcessingStatus `json:"status,omitempty"`
	Message          string                  `json:"message,omitempty"`
	RunId            model.RunId             `json:"runid,omitempty"`
	DetectedVersion  string                  `json:"detectedVersion,omitempty"`
	ObservedVersion  string                  `json:"observedVersion,omitempty"`
	EffectiveVersion string                  `json:"effectiveVersion,omitempty"`

	Result *int `json:"result,omitempty"`
}

func NewOperatorNode(ns, n string, op OperatorName, operands ...string) *Operator {
	return &Operator{
		GenerationObjectMeta: database.NewGenerationObjectMeta(mymetamodel.TYPE_OPERATOR, ns, n),
		Spec: OperatorSpec{
			Operator: op,
			Operands: slices.Clone(operands),
		},
	}
}
