package db

import (
	"slices"

	"github.com/mandelsoft/engine/pkg/database"
	"github.com/mandelsoft/engine/pkg/processing/model"
	"github.com/mandelsoft/engine/pkg/processing/model/support"

	mymetamodel "github.com/mandelsoft/engine/pkg/metamodels/foreigndemo"
)

func init() {
	database.MustRegisterType[Expression, support.DBObject](Scheme) // Goland requires second type parameter
}

type Expression struct {
	database.GenerationObjectMeta

	Spec   ExpressionSpec   `json:"spec"`
	Status ExpressionStatus `json:"status"`
}

var _ database.Object = (*Value)(nil)

func (n *Expression) GetStatusValue() string {
	return string(n.Status.Status)
}

type ExpressionSpec struct {
	Operands    map[string]int           `json:"operands,omitempty"`
	Expressions map[string]ExpressionDef `json:"expressions,omitempty"`
}

func (e *ExpressionSpec) GetVersion() string {
	return support.NewState(e).GetVersion()
}

type ExpressionDef struct {
	Operands []string `json:"operands,omitempty"`
	Operator OperatorName
}

type ExpressionStatus struct {
	Status          model.Status     `json:"status,omitempty"`
	Message         string           `json:"message,omitempty"`
	ObservedVersion string           `json:"observedVersion,omitempty"`
	Output          ExpressionOutput `json:"output,omitempty"`
}

type ExpressionOutput map[string]int

func NewExpression(ns, n string) *Expression {
	return &Expression{
		GenerationObjectMeta: database.NewGenerationObjectMeta(mymetamodel.TYPE_EXPRESSION, ns, n),
		Spec: ExpressionSpec{
			Operands:    map[string]int{},
			Expressions: map[string]ExpressionDef{},
		},
	}
}

func (e *ExpressionSpec) AddOperand(name string, value int) *ExpressionSpec {
	e.Operands[name] = value
	return e
}

func (e *ExpressionSpec) AddOperation(name string, op OperatorName, operands ...string) *ExpressionSpec {
	e.Expressions[name] = ExpressionDef{
		Operands: slices.Clone(operands),
		Operator: op,
	}
	return e
}

func (e *Expression) GetStatus() string {
	return string(e.Status.Status)
}

func (e *Expression) AddOperand(name string, value int) *Expression {
	e.Spec.AddOperand(name, value)
	return e
}

func (e *Expression) AddOperation(name string, op OperatorName, operands ...string) *Expression {
	e.Spec.AddOperation(name, op, operands...)
	return e
}
