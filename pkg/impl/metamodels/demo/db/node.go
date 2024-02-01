package db

import (
	"slices"

	"github.com/mandelsoft/engine/pkg/database"
	"github.com/mandelsoft/engine/pkg/metamodel/common"
	"github.com/mandelsoft/engine/pkg/metamodel/model"
	"github.com/mandelsoft/engine/pkg/metamodel/model/support"
	mymetamodel "github.com/mandelsoft/engine/pkg/metamodels/demo"
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

type OperatorName string

const OP_ADD = OperatorName("add")
const OP_SUB = OperatorName("sub")
const OP_MUL = OperatorName("mul")
const OP_DIV = OperatorName("div")

type NodeSpec struct {
	Value    *int          `json:"value,omitempty"`
	Operator *OperatorName `json:"operator,omitempty"`
	Operands []string      `json:"operands,omitempty"`
}

type NodeStatus struct {
	Status           common.ProcessingStatus `json:"status,omitempty"`
	Message          string                  `json:"message,omitempty"`
	RunId            model.RunId             `json:"runid,omitempty"`
	DetectedVersion  string                  `json:"detectedVersion,omitempty"`
	ObservedVersion  string                  `json:"observedVersion,omitempty"`
	EffectiveVersion string                  `json:"effectiveVersion,omitempty"`

	Result *int `json:"result,omitempty"`
}

func NewOperatorNode(ns, n string, op OperatorName, operands ...string) *Node {
	return &Node{
		GenerationObjectMeta: database.NewGenerationObjectMeta(mymetamodel.TYPE_NODE, ns, n),
		Spec: NodeSpec{
			Operator: utils.Pointer(op),
			Operands: slices.Clone(operands),
		},
	}
}

func NewValueNode(ns, n string, value int) *Node {
	return &Node{
		GenerationObjectMeta: database.NewGenerationObjectMeta(mymetamodel.TYPE_NODE, ns, n),
		Spec: NodeSpec{
			Value: utils.Pointer(value),
		},
	}
}
