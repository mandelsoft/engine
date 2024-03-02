package db

import (
	"slices"

	. "github.com/mandelsoft/engine/pkg/processing/mmids"

	"github.com/mandelsoft/engine/pkg/database"
	"github.com/mandelsoft/engine/pkg/processing/model"
	"github.com/mandelsoft/engine/pkg/processing/model/support/db"

	mymetamodel "github.com/mandelsoft/engine/pkg/metamodels/valopdemo"
)

func init() {
	database.MustRegisterType[Operator, db.Object](Scheme) // Goland requires second type parameter
}

type Operator struct {
	db.ObjectMeta

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
	Operator OperatorName `json:"operator"`
	Operands []string     `json:"operands,omitempty"`
}

type OperatorStatus struct {
	Phase            Phase        `json:"phase,omitempty"`
	Status           model.Status `json:"status,omitempty"`
	Message          string       `json:"message,omitempty"`
	RunId            RunId        `json:"runid,omitempty"`
	DetectedVersion  string       `json:"detectedVersion,omitempty"`
	ObservedVersion  string       `json:"observedVersion,omitempty"`
	EffectiveVersion string       `json:"effectiveVersion,omitempty"`

	Result *int `json:"result,omitempty"`
}

func NewOperatorNode(ns, n string, op OperatorName, operands ...string) *Operator {
	return &Operator{
		ObjectMeta: db.NewObjectMeta(mymetamodel.TYPE_OPERATOR, ns, n),
		Spec: OperatorSpec{
			Operator: op,
			Operands: slices.Clone(operands),
		},
	}
}
