package db

import (
	"github.com/mandelsoft/engine/pkg/database"
	"github.com/mandelsoft/engine/pkg/metamodel/model"
	"github.com/mandelsoft/engine/pkg/metamodel/model/support"
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

const OP_ADD = "add"
const OP_SUB = "sub"
const OP_MUL = "mul"
const OP_DIV = "div"

type NodeSpec struct {
	Value    *int     `json:"value,omitempty"`
	Operator *string  `json:"operator,omitempty"`
	Operands []string `json:"operands,omitempty"`
}

type NodeStatus struct {
	State            string      `json:"state,omitempty"`
	Message          string      `json:"message,omitempty"`
	RunId            model.RunId `json:"runid,omitempty"`
	DetectedVersion  string      `json:"detectedVersion,omitempty"`
	ObservedVersion  string      `json:"observedVersion,omitempty"`
	EffectiveVersion string      `json:"effectiveVersion,omitempty"`

	Result *int `json:"result,omitempty"`
}
