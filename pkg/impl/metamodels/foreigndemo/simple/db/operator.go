package db

import (
	"fmt"
	"slices"
	"strings"

	mymetamodel "github.com/mandelsoft/engine/pkg/metamodels/foreigndemo"
	. "github.com/mandelsoft/engine/pkg/processing/mmids"
	"github.com/mandelsoft/engine/pkg/processing/model"
	"github.com/mandelsoft/engine/pkg/processing/model/support/db"

	"github.com/mandelsoft/engine/pkg/database"
)

func init() {
	database.MustRegisterType[Operator, db.DBObject](Scheme) // Goland requires second type parameter
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
	Operands   map[string]string `json:"operands,omitempty"`
	Operations Operations        `json:"operations,omitempty"`
	Outputs    map[string]string `json:"outputs"`
}

type Operations map[string]Operation

func (o Operations) String() string {
	return fmt.Sprintf("%#v", map[string]Operation(o))
}

type Operation struct {
	Operator OperatorName `json:"operator"`
	Operands []string     `json:"operands,omitempty"`
}

func (o Operation) String() string {
	return fmt.Sprintf("%s: %s", o.Operator, strings.Join(o.Operands, ", "))
}

type OperatorStatus struct {
	Phase            Phase        `json:"phase,omitempty"`
	Status           model.Status `json:"status,omitempty"`
	Message          string       `json:"message,omitempty"`
	RunId            RunId        `json:"runid,omitempty"`
	DetectedVersion  string       `json:"detectedVersion,omitempty"`
	ObservedVersion  string       `json:"observedVersion,omitempty"`
	EffectiveVersion string       `json:"effectiveVersion,omitempty"`

	Result ExposeOutput `json:"result,omitempty"`
}

type ExposeOutput map[string]int

func NewOperatorNode(ns, n string) *Operator {
	return &Operator{
		ObjectMeta: db.NewObjectMeta(mymetamodel.TYPE_OPERATOR, ns, n),
		Spec: OperatorSpec{
			Operands:   map[string]string{},
			Operations: map[string]Operation{},
			Outputs:    map[string]string{},
		},
	}
}

func (o *Operator) AddOperand(name string, source string) *Operator {
	o.Spec.Operands[name] = source
	return o
}

func (o *Operator) AddOperation(name string, op OperatorName, operands ...string) *Operator {
	o.Spec.Operations[name] = Operation{
		Operator: op,
		Operands: slices.Clone(operands),
	}
	return o
}

func (o *Operator) AddOutput(name string, source string) *Operator {
	o.Spec.Outputs[name] = source
	return o
}
