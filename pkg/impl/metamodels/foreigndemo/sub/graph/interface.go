package graph

import (
	"github.com/mandelsoft/engine/pkg/database"
	"github.com/mandelsoft/engine/pkg/processing/metamodel"
	. "github.com/mandelsoft/engine/pkg/processing/mmids"

	"github.com/mandelsoft/engine/pkg/version"
)

type Node interface {
	database.Object

	SubGraph() []version.Node
}

func GraphIdForPhase(id database.ObjectId, phase Phase) version.Id {
	// Graphs are always local to a namespace.
	return version.NewId(NewTypeId(id.GetType()+"State", phase), id.GetName())
}

func GraphId(typ string, name string, phase Phase) version.Id {
	// Graphs are always local to a namespace.
	return version.NewId(NewTypeId(typ, phase), name)
}

type Graph interface {
	version.EvaluatedGraph
}

type graph struct {
	version.EvaluatedGraph
}

func NewGraph(cmp version.Composer, nodes ...Node) (Graph, error) {
	g := version.NewGraph()
	for _, s := range nodes {
		for _, n := range s.SubGraph() {
			g.AddNode(n)
		}
	}
	e, err := version.EvaluateGraph(g, cmp)
	if err != nil {
		return nil, err
	}
	return &graph{e}, nil
}

func MapToPhaseId(oid database.ObjectId, mm metamodel.MetaModel) version.Id {
	t := mm.GetExternalType(oid.GetType())
	if t == nil {
		return nil
	}
	trigger := t.Trigger()
	if trigger == nil {
		return nil
	}
	return version.NewId(trigger.Id(), oid.GetName())
}
