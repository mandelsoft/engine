package db

import (
	"encoding/json"
	"slices"

	"github.com/mandelsoft/engine/pkg/database"
	mymetamodel "github.com/mandelsoft/engine/pkg/metamodels/foreigndemo"
	"github.com/mandelsoft/engine/pkg/processing"
	"github.com/mandelsoft/engine/pkg/processing/model"
	"github.com/mandelsoft/engine/pkg/processing/model/support/db"
)

func init() {
	database.MustRegisterType[Expression, db.Object](Scheme) // Goland requires second type parameter
}

type Expression struct {
	db.ObjectMeta

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

func NewExpressionSpec() *ExpressionSpec {
	return &ExpressionSpec{
		Operands:    map[string]int{},
		Expressions: map[string]ExpressionDef{},
	}
}

func (e *ExpressionSpec) GetVersion() string {
	return processing.NewState(e).GetVersion()
}

func (e *ExpressionSpec) GetDescription() string {
	d, _ := json.Marshal(e)
	return string(d)
}

type ExpressionDef struct {
	Operands   []string     `json:"operands,omitempty"`
	Operator   OperatorName `json:"operator,omitempty"`
	Expression string       `json:"expression,omitempty"`
}

type ExpressionStatus struct {
	// ExpressionStateSpec is the local object state part held in the state object
	// and propagated as part of the status in the external object.
	ExpressionStateSpec `json:",inline"`

	Status          model.Status     `json:"status,omitempty"`
	Message         string           `json:"message,omitempty"`
	ObservedVersion string           `json:"observedVersion,omitempty"`
	Output          ExpressionOutput `json:"output,omitempty"`

	Generated GeneratedObjects `json:"generatedExpressions,omitempty"`
}

type GeneratedObjects struct {
	Namespace string                    `json:"namespace,omitempty"`
	Objects   []database.LocalObjectRef `json:"objects,omitempty"`
	Deleting  []database.LocalObjectRef `json:"deleting,omitempty"`
	Results   []database.LocalObjectRef `json:"results,omitempty"`
}

type ElementName struct {
	Type string `json:"type"`
	Name string `json:"name"`
}

type ExpressionOutput map[string]int

func (o ExpressionOutput) Description() string {
	d, _ := json.Marshal(o)
	return string(d)
}

func NewExpression(ns, n string) *Expression {
	return &Expression{
		ObjectMeta: db.NewObjectMeta(mymetamodel.TYPE_EXPRESSION, ns, n),
		Spec:       *NewExpressionSpec(),
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

func (e *ExpressionSpec) AddExpressionOperation(name string, expr string) *ExpressionSpec {
	e.Expressions[name] = ExpressionDef{
		Operator:   OP_EXPR,
		Expression: expr,
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

func (e *Expression) AddExpressionOperation(name string, expr string) *Expression {
	e.Spec.AddExpressionOperation(name, expr)
	return e
}
