package graph

import (
	"github.com/mandelsoft/engine/pkg/database"
	"github.com/mandelsoft/engine/pkg/impl/metamodels/foreigndemo/sub/db"
	mymetamodel "github.com/mandelsoft/engine/pkg/metamodels/foreigndemo"
	"github.com/mandelsoft/engine/pkg/processing/model"
	"github.com/mandelsoft/engine/pkg/processing/model/support"
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

func (v *Value) Object() database.Object {
	return v.Value
}

func (v *Value) DBUpdate(o database.Object) bool {
	op := o.(*db.Value)
	mod := false
	support.UpdateField(&op.Spec, &v.Spec, &mod)
	return mod
}

func (v *Value) DBCheck(g Graph, o database.Object) (bool, model.Status, error) {
	op := o.(*db.Value)
	if op.Status.FormalVersion == g.FormalVersion(GraphIdForPhase(o, mymetamodel.FINAL_VALUE_PHASE)) {
		return true, op.Status.Status, nil
	}
	if op.Status.DetectedVersion == utils.HashData(v.Spec) {
		return true, op.Status.Status, nil
	}
	return false, "", nil
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
