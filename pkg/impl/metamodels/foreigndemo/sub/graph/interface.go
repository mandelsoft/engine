package graph

import (
	"slices"

	"github.com/mandelsoft/engine/pkg/database"
	"github.com/mandelsoft/engine/pkg/processing/metamodel"
	. "github.com/mandelsoft/engine/pkg/processing/mmids"
	"github.com/mandelsoft/engine/pkg/processing/model"
	"github.com/mandelsoft/engine/pkg/processing/model/support"
	"github.com/mandelsoft/engine/pkg/processing/model/support/db"
	"github.com/mandelsoft/engine/pkg/utils"
	"github.com/mandelsoft/logging"

	"github.com/mandelsoft/engine/pkg/version"
)

type Node interface {
	database.Object

	Object() database.Object

	DBUpdate(o database.Object) bool
	DBCheck(log logging.Logger, g Graph, o database.Object) (bool, model.Status, error)

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

	Objects() []database.ObjectId
	HasObject(id database.ObjectId) bool
	IsEmpty() bool

	UpdateDB(log logging.Logger, odb database.Database[db.Object]) (bool, error)
	CheckDB(log logging.Logger, odb database.Database[db.Object]) (bool, model.Status, error)
}

type graph struct {
	nodes map[database.ObjectId]Node
	version.EvaluatedGraph
}

func NewGraph(cmp version.Composer, nodes ...Node) (Graph, error) {
	g := version.NewGraph()
	rsc := map[database.ObjectId]Node{}
	for _, s := range nodes {
		rsc[database.NewObjectIdFor(s)] = s
		for _, n := range s.SubGraph() {
			g.AddNode(n)
		}
	}
	e, err := version.EvaluateGraph(g, cmp)
	if err != nil {
		return nil, err
	}
	return &graph{rsc, e}, nil
}

func (g *graph) IsEmpty() bool {
	return len(g.nodes) == 0
}

func (g *graph) Objects() []database.ObjectId {
	ids := utils.MapKeys(g.nodes, database.CompareObjectId)
	slices.SortFunc(ids, database.CompareObjectId)
	return ids
}

func (g *graph) HasObject(id database.ObjectId) bool {
	return g.nodes[database.NewObjectIdFor(id)] != nil
}

func (g *graph) UpdateDB(log logging.Logger, odb database.Database[db.Object]) (bool, error) {
	objs := g.Objects()
	log.Info("update generated expression graph on db", "ids", objs)
	mod := false
	for _, id := range objs {
		n := g.nodes[id]
		o := n.Object()
		m, err := database.CreateOrModify(odb, &o, func(o database.Object) bool { return n.DBUpdate(o) })
		if err != nil {
			log.LogError(err, "- updated object {{oid}} failed {{error}}", "oid", id)
			return mod, err
		}
		if m {
			log.Info("- updated object {{oid}}", "oid", id)
		} else {
			log.Debug("- unchanged object {{oid}}", "oid", id)
		}
		mod = mod || m
	}
	return mod, nil
}

func (g *graph) CheckDB(log logging.Logger, odb database.Database[db.Object]) (bool, model.Status, error) {
	log.Info("checking generated expression graph on db")
	rstatus := model.STATUS_INITIAL
	final := true
	for _, id := range g.Objects() {
		n := g.nodes[id]
		o, err := odb.GetObject(id)
		if err != nil {
			return false, "", err
		}

		ok, status, err := n.DBCheck(log, g, o)
		if err != nil {
			return false, "", err
		}
		if ok {
			log.Info("- found status {{status}} for {{oid}}", "status", status, "oid", id)
			rstatus = support.MergeStatus(rstatus, status)
		} else {
			log.Info("- {{oid}} still processing ({{status}}", "status", status, "oid", id)
			final = false
		}
	}
	return final, rstatus, nil
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
