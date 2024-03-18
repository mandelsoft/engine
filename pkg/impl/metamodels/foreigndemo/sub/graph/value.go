package graph

import (
	"github.com/mandelsoft/engine/pkg/database"
	"github.com/mandelsoft/engine/pkg/processing/graph"
	"github.com/mandelsoft/engine/pkg/processing/model"
	"github.com/mandelsoft/engine/pkg/processing/model/support"
	"github.com/mandelsoft/engine/pkg/version"
	"github.com/mandelsoft/goutils/general"
	"github.com/mandelsoft/logging"

	"github.com/mandelsoft/engine/pkg/impl/metamodels/foreigndemo/sub/db"
	mymetamodel "github.com/mandelsoft/engine/pkg/metamodels/foreigndemo"
)

func init() {
	Scheme.Register(mymetamodel.TYPE_VALUE, &ValueCheck{})
}

type Value struct {
	*db.Value
	input string
}

var _ Node = (*Value)(nil)

func NewValue(v *db.Value, input ...string) *Value {
	return &Value{
		Value: v,
		input: general.Optional(input...),
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

func (v *Value) SubGraph(g Graph) []version.Node {
	var deps []version.Id

	vers := ""
	if v.input == "" {
		vers = general.HashData(v.Spec)
	} else {
		deps = []version.Id{GraphId(mymetamodel.TYPE_OPERATOR, v.input, mymetamodel.PHASE_EXPOSE)}
	}
	return []version.Node{version.NewNodeById(g.GraphIdForPhase(v, mymetamodel.PHASE_PROPAGATE), vers, deps...)}
}

////////////////////////////////////////////////////////////////////////////////

type ValueCheck struct {
	graph.DefaultCheckNode[*Value]
}

func (v *ValueCheck) DBCheck(log logging.Logger, g Graph, o database.Object) (bool, model.Status, error) {
	op := o.(*db.Value)

	if v.Configured != nil {
		exp := general.HashData(v.Configured.Spec)
		if op.Status.DetectedVersion != exp {
			log.Debug("  detected version not yet reached (expected {{expected}}, found {{found}})", "expected", exp, "found", op.Status.DetectedVersion)
			return false, "", nil
		}
	}

	exp := g.FormalVersion(g.GraphIdForPhase(o, mymetamodel.FINAL_VALUE_PHASE))
	if op.Status.FormalVersion == g.FormalVersion(g.GraphIdForPhase(o, mymetamodel.FINAL_VALUE_PHASE)) {
		return true, op.Status.Status, nil
	}
	log.Debug("  formal version not yet reached (expected {{expected}}, found {{found}})", "expected", exp, "found", op.Status.FormalVersion)

	return false, "", nil
}
