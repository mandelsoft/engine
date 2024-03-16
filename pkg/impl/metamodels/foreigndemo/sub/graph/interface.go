package graph

import (
	mymetamodel "github.com/mandelsoft/engine/pkg/metamodels/foreigndemo"
	"github.com/mandelsoft/engine/pkg/processing/graph"
	"github.com/mandelsoft/engine/pkg/version"
)

type Graph = graph.Graph
type Node = graph.Node

var GraphId = graph.GraphId

var MetaModel = mymetamodel.MustMetaModel("")

func NewGraph(cmp version.Composer, nodes ...graph.Node) (graph.Graph, error) {
	return graph.NewGraph(MetaModel, Scheme, cmp, nodes...)
}
