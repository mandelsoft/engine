package graph

import (
	"github.com/mandelsoft/engine/pkg/impl/metamodels/foreigndemo/sub/db"
	mymetamodel "github.com/mandelsoft/engine/pkg/metamodels/foreigndemo"
	"github.com/mandelsoft/engine/pkg/utils"
	"github.com/mandelsoft/engine/pkg/version"
)

type Value struct {
	*db.Value
	input string
}

var _ Node = (*Value)(nil)

func NewValue(v *db.Value, input ...string) *Value {
	return &Value{
		Value: v,
		input: utils.Optional(input...),
	}
}

func (v *Value) SubGraph() []version.Node {
	var deps []version.Id

	vers := ""
	if v.input == "" {
		vers = utils.HashData(v.Spec)
	} else {
		deps = []version.Id{GraphId(mymetamodel.TYPE_OPERATOR, v.input, mymetamodel.PHASE_EXPOSE)}
	}
	return []version.Node{version.NewNodeById(GraphIdForPhase(v, mymetamodel.PHASE_PROPAGATE), vers, deps...)}
}
