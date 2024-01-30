package db

import (
	"slices"

	"github.com/mandelsoft/engine/pkg/database"
	"github.com/mandelsoft/engine/pkg/metamodel/common"
	"github.com/mandelsoft/engine/pkg/metamodel/model"
	"github.com/mandelsoft/engine/pkg/metamodel/model/support"
	"github.com/mandelsoft/engine/pkg/metamodels/multidemo"
	"github.com/mandelsoft/engine/pkg/utils"
)

func init() {
	database.MustRegisterType[Node, support.DBObject](Scheme) // Goland requires second type parameter
}

type Node struct {
	database.GenerationObjectMeta

	Spec   NodeSpec   `json:"spec"`
	Status NodeStatus `json:"status"`
}

var _ database.Object = (*Node)(nil)

type Operator string

const OP_ADD = Operator("add")
const OP_SUB = Operator("sub")
const OP_MUL = Operator("mul")
const OP_DIV = Operator("div")

type NodeSpec struct {
	Value    *int      `json:"value,omitempty"`
	Operator *Operator `json:"operator,omitempty"`
	Operands []string  `json:"operands,omitempty"`
}

type NodeStatus struct {
	Phase            model.Phase             `json:"phase,omitempty"`
	Status           common.ProcessingStatus `json:"status,omitempty"`
	Message          string                  `json:"message,omitempty"`
	RunId            model.RunId             `json:"runid,omitempty"`
	DetectedVersion  string                  `json:"detectedVersion,omitempty"`
	ObservedVersion  string                  `json:"observedVersion,omitempty"`
	EffectiveVersion string                  `json:"effectiveVersion,omitempty"`

	Result *int `json:"result,omitempty"`
}

func NewOperatorNode(ns, n string, op Operator, operands ...string) *Node {
	return &Node{
		GenerationObjectMeta: database.NewGenerationObjectMeta(multidemo.TYPE_NODE, ns, n),
		Spec: NodeSpec{
			Operator: utils.Pointer(op),
			Operands: slices.Clone(operands),
		},
	}
}

func NewValueNode(ns, n string, value int) *Node {
	return &Node{
		GenerationObjectMeta: database.NewGenerationObjectMeta(multidemo.TYPE_NODE, ns, n),
		Spec: NodeSpec{
			Value: utils.Pointer(value),
		},
	}
}
