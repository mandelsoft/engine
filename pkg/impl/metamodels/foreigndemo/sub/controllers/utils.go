package controllers

import (
	"slices"
	"strconv"

	"github.com/mandelsoft/engine/pkg/database"
	"github.com/mandelsoft/engine/pkg/impl/metamodels/foreigndemo/sub/db"
	"github.com/mandelsoft/engine/pkg/impl/metamodels/foreigndemo/sub/graph"
	"github.com/mandelsoft/engine/pkg/utils"
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
		return g.HasObject(database.NewObjectId(l.GetType(), o.Status.Generated.Namespace, l.Name))
	})
}

func NewRefs(o *db.Expression, g graph.Graph) []database.LocalObjectRef {
	n := utils.TransformSlice(g.Objects(), database.NewLocalObjectRefFor)
	return utils.FilterSlice(o.Status.Generated.Objects, func(l database.LocalObjectRef) bool {
		return !slices.Contains(n, l)
	})
}
