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

type ExpressionSpec struct {
	Operands    map[string]int           `json:"operands,omitempty"`
	Expressions map[string]ExpressionDef `json:"expressions,omitempty"`
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

func (e *Expression) AddOperand(name string, value int) *Expression {
	e.Spec.Operands[name] = value
	return e
}

func (e *Expression) AddExpression(name string, op OperatorName, operands ...string) *Expression {
	e.Spec.Expressions[name] = ExpressionDef{
		Operands: slices.Clone(operands),
		Operator: op,
	}
	return e
}
