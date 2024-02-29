package graph

import (
	"strconv"

	"github.com/mandelsoft/engine/pkg/database"
	"github.com/mandelsoft/engine/pkg/impl/metamodels/foreigndemo/sub/db"
	mymetamodel "github.com/mandelsoft/engine/pkg/metamodels/foreigndemo"
	"github.com/mandelsoft/engine/pkg/processing/model"
	"github.com/mandelsoft/engine/pkg/processing/model/support"
	"github.com/mandelsoft/engine/pkg/utils"
	"github.com/mandelsoft/engine/pkg/version"
	"github.com/mandelsoft/logging"
)

type Operator struct {
	*db.Operator
}

var _ Node = (*Operator)(nil)

func NewOperator(v *db.Operator) *Operator {
	return &Operator{
		Operator: v,
	}
}

func (v *Operator) Object() database.Object {
	return v.Operator
}

func (v *Operator) DBUpdate(o database.Object) bool {
	op := o.(*db.Operator)
	mod := false
	support.UpdateField(&op.Spec, &v.Spec, &mod)
	return mod
}

func (v *Operator) DBCheck(log logging.Logger, g Graph, o database.Object) (bool, model.Status, error) {
	op := o.(*db.Operator)

	exp := utils.HashData(v.Spec)
	if op.Status.DetectedVersion != exp {
		log.Debug("  detected version not yet reached (expected {{expected}}, found {{found}})", "expected", exp, "found", op.Status.DetectedVersion)
		return false, "", nil
	}

	exp = g.FormalVersion(GraphIdForPhase(o, op.Status.Phase))
	fvmatch := op.Status.FormalVersion == exp

	if !fvmatch {
		log.Debug("  formal version od phase {{phase}} not yet reached (expected {{expected}}, found {{found}})", "phase", op.Status.Phase, "expected", exp, "found", op.Status.DetectedVersion)
	}
	switch op.Status.Phase {
	case mymetamodel.PHASE_GATHER:
		if fvmatch && op.Status.Status != model.STATUS_COMPLETED {
			return true, op.Status.Status, nil
		}
	case mymetamodel.PHASE_EXPOSE:
		if fvmatch {
			return true, op.Status.Status, nil
		}
	}
	return false, "", nil
}

func (v *Operator) SubGraph() []version.Node {
	var deps []version.Id
	for _, d := range v.Spec.Operands {
		if _, err := strconv.Atoi(d); err != nil {
			deps = append(deps, GraphId(mymetamodel.TYPE_VALUE_STATE, d, mymetamodel.PHASE_PROPAGATE))
		}
	}

	gather := GraphIdForPhase(v, mymetamodel.PHASE_GATHER)
	expression := GraphId(mymetamodel.TYPE_EXPRESSION_STATE, v.GetName(), mymetamodel.PHASE_CALCULATE)
	expose := GraphIdForPhase(v, mymetamodel.PHASE_EXPOSE)

	r := []version.Node{
		version.NewNodeById(gather, utils.HashData(v.Spec), deps...),
		version.NewNodeById(expression, "", gather),
		version.NewNodeById(expose, "", gather, expression),
	}

	for o := range v.Spec.Outputs {
		id := GraphId(mymetamodel.TYPE_VALUE_STATE, o, mymetamodel.PHASE_PROPAGATE)
		r = append(r, version.NewNodeById(id, "", expose))
	}
	return r
}
