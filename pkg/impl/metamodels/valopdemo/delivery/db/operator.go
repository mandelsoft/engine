package db

import (
	"slices"

	. "github.com/mandelsoft/engine/pkg/processing/mmids"
	"github.com/mandelsoft/engine/pkg/processing/model"

	"github.com/mandelsoft/engine/pkg/database"
	"github.com/mandelsoft/engine/pkg/processing/model/support"

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

func (n *Operator) GetStatusValue() string {
	return string(n.Status.Status)
}

type OperatorName string

const OP_ADD = OperatorName("add")
const OP_SUB = OperatorName("sub")
const OP_MUL = OperatorName("mul")
const OP_DIV = OperatorName("div")

type OperatorSpec struct {
	Operands   []string    `json:"operands,omitempty"`
	Operations []Operation `json:"operations,omitempty"`
}

type Operation struct {
	Operator OperatorName `json:"operator"`
	Target   string       `json:"target"`
}

type OperatorStatus struct {
	Phase            Phase        `json:"phase,omitempty"`
	Status           model.Status `json:"status,omitempty"`
	Message          string       `json:"message,omitempty"`
	RunId            RunId        `json:"runid,omitempty"`
	DetectedVersion  string       `json:"detectedVersion,omitempty"`
	ObservedVersion  string       `json:"observedVersion,omitempty"`
	EffectiveVersion string       `json:"effectiveVersion,omitempty"`

	Result CalculationOutput `json:"result,omitempty"`
}

type CalculationOutput map[string]int

func NewOperatorNode(ns, n string, operands ...string) *Operator {
	return &Operator{
		GenerationObjectMeta: database.NewGenerationObjectMeta(mymetamodel.TYPE_OPERATOR, ns, n),
		Spec: OperatorSpec{
			Operands: slices.Clone(operands),
		},
	}
}

func (o *Operator) AddOperation(op OperatorName, target string) *Operator {
	o.Spec.Operations = append(o.Spec.Operations, Operation{Operator: op, Target: target})
	return o
}
