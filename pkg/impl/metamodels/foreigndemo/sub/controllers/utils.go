package controllers

import (
	"slices"
	"strconv"

	"github.com/mandelsoft/engine/pkg/database"
	"github.com/mandelsoft/engine/pkg/impl/metamodels/foreigndemo/sub/db"
	"github.com/mandelsoft/engine/pkg/impl/metamodels/foreigndemo/sub/graph"
	"github.com/mandelsoft/engine/pkg/utils"
	"github.com/mandelsoft/logging"
)

type Values map[string]int

func (v Values) Get(op string) (int, bool) {
	i, err := strconv.Atoi(op)
	if err == nil {
		return i, true
	}
	i, ok := v[op]
	return i, ok
}

func (v Values) IsComplete(elems map[string]*ExpressionInfo) bool {
	for k := range elems {
		if _, ok := v[k]; !ok {
			return false
		}
	}
	return true
}

func (v Values) Missing(elems map[string]*ExpressionInfo) []string {
	var r []string
	for k := range elems {
		if _, ok := v[k]; !ok {
			r = append(r, k)
		}
	}
	return r
}

func OldRefs(o *db.Expression, g graph.Graph) []database.LocalObjectRef {
	return utils.FilterSlice(o.Status.Generated.Objects, func(l database.LocalObjectRef) bool {
		return !g.HasObject(database.NewObjectId(l.GetType(), o.Status.Generated.Namespace, l.Name))
	})
}

func NewRefs(o *db.Expression, g graph.Graph) []database.LocalObjectRef {
	n := utils.TransformSlice(g.Objects(), database.NewLocalObjectRefFor)
	return utils.FilterSlice(n, func(l database.LocalObjectRef) bool {
		return !slices.Contains(o.Status.Generated.Objects, l)
	})
}

func GenerateGraph(log logging.Logger, e *db.Expression, namespace string) (graph.Graph, error) {
	infos, order, err := Validate(e)
	if err != nil {
		return nil, err
	}
	values := map[string]int{}
	err = PreCalc(log, order, infos, values)
	if err != nil {
		return nil, err
	}
	return Generate(log, namespace, infos, values)
}
