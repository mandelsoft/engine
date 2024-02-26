package graph

import (
	"strconv"

	"github.com/mandelsoft/engine/pkg/impl/metamodels/foreigndemo/sub/db"
	mymetamodel "github.com/mandelsoft/engine/pkg/metamodels/foreigndemo"
	"github.com/mandelsoft/engine/pkg/utils"
	"github.com/mandelsoft/engine/pkg/version"
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
