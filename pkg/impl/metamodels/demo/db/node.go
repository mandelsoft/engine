package db

import (
	"slices"

	. "github.com/mandelsoft/engine/pkg/processing/mmids"
	"github.com/mandelsoft/engine/pkg/processing/model/support/db"

	"github.com/mandelsoft/engine/pkg/database"
	mymetamodel "github.com/mandelsoft/engine/pkg/metamodels/demo"
	"github.com/mandelsoft/engine/pkg/processing/model"
	"github.com/mandelsoft/engine/pkg/utils"
)

func init() {
	database.MustRegisterType[Node, db.Object](Scheme) // Goland requires second type parameter
}

type Node struct {
	db.ObjectMeta

	Spec   NodeSpec   `json:"spec"`
	Status NodeStatus `json:"status"`
}

var _ database.Object = (*Node)(nil)

func (n *Node) GetStatusValue() string {
	return string(n.Status.Status)
}

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
	Status           model.Status `json:"status,omitempty"`
	Message          string       `json:"message,omitempty"`
	RunId            RunId        `json:"runid,omitempty"`
	DetectedVersion  string       `json:"detectedVersion,omitempty"`
	ObservedVersion  string       `json:"observedVersion,omitempty"`
	EffectiveVersion string       `json:"effectiveVersion,omitempty"`

	Result *int `json:"result,omitempty"`
}

func NewOperatorNode(ns, n string, op OperatorName, operands ...string) *Node {
	return &Node{
		ObjectMeta: db.NewObjectMeta(mymetamodel.TYPE_NODE, ns, n),
		Spec: NodeSpec{
			Operator: utils.Pointer(op),
			Operands: slices.Clone(operands),
		},
	}
}

func NewValueNode(ns, n string, value int) *Node {
	return &Node{
		ObjectMeta: db.NewObjectMeta(mymetamodel.TYPE_NODE, ns, n),
		Spec: NodeSpec{
			Value: utils.Pointer(value),
		},
	}
}
