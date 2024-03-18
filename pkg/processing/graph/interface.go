package graph

import (
	"slices"
	"strings"

	"github.com/mandelsoft/engine/pkg/database"
	"github.com/mandelsoft/engine/pkg/processing/metamodel"
	. "github.com/mandelsoft/engine/pkg/processing/mmids"
	"github.com/mandelsoft/engine/pkg/processing/model"
	"github.com/mandelsoft/engine/pkg/processing/model/support/db"
	"github.com/mandelsoft/engine/pkg/runtime"
	"github.com/mandelsoft/engine/pkg/version"
	"github.com/mandelsoft/goutils/maputils"
	"github.com/mandelsoft/goutils/sliceutils"
	"github.com/mandelsoft/logging"
)

type CheckNode interface {
	database.Object

	SetNode(Node)

	// DBCheck is called to check, whether the object has reached its desired formal
	// graph version.
	DBCheck(log logging.Logger, g Graph, o database.Object) (bool, model.Status, error)
}

type CheckScheme = runtime.TypeScheme[CheckNode]

func NewCheckScheme() CheckScheme {
	return runtime.NewTypeScheme[CheckNode]()
}

////////////////////////////////////////////////////////////////////////////////

type Node interface {
	database.Object

	Object() database.Object

	// DBUpdate is called to update the space of an object on the object base.
	DBUpdate(o database.Object) bool

	SubGraph(Graph) []version.Node
}

func (g *graph) GraphIdForPhase(id database.ObjectId, phase Phase) version.Id {
	// Graphs are always local to a namespace.
	t := g.mm.GetExternalType(id.GetType()).Trigger().Id().GetType()
	return version.NewId(NewTypeId(t, phase), id.GetName())
}

type Graph interface {
	version.EvaluatedGraph

	RootObjects() []database.ObjectId
	HasRootObject(id database.ObjectId) bool

	CheckObjects() []database.ObjectId

	Objects() []database.ObjectId
	HasObject(id database.ObjectId) bool
	GetObject(id database.ObjectId) database.Object
	IsEmpty() bool

	UpdateDB(log logging.Logger, odb database.Database[db.Object]) (bool, error)
	CheckDB(log logging.Logger, odb database.Database[db.Object]) (bool, model.Status, error)

	GraphIdForPhase(id database.ObjectId, phase Phase) version.Id
	MapToPhaseId(oid database.ObjectId) version.Id
}

type graph struct {
	mm     metamodel.MetaModel
	scheme CheckScheme
	nodes  map[database.ObjectId]Node
	roots  []database.ObjectId
	checks []CheckNode
	version.EvaluatedGraph
}

func NewGraph(mm metamodel.MetaModel, scheme CheckScheme, cmp version.Composer, nodes ...Node) (Graph, error) {
	g := &graph{
		mm:     mm,
		scheme: scheme,
		nodes:  map[database.ObjectId]Node{},
	}
	vg := version.NewGraph()
	for _, s := range nodes {
		g.nodes[database.NewObjectIdFor(s)] = s
		for _, n := range s.SubGraph(g) {
			vg.AddNode(n)
		}
	}
	e, err := version.EvaluateGraph(vg, cmp)
	if err != nil {
		return nil, err
	}

	g.EvaluatedGraph = e
	for _, n := range g.nodes {
		c, err := g.createCheck(n)
		if err != nil {
			return nil, err
		}
		g.checks = append(g.checks, c)
	}
	slices.SortFunc(g.checks, database.CompareObject[CheckNode])
	g.roots = g.rootObjects()
	for _, n := range g.roots {
		if !slices.ContainsFunc(g.checks, database.MatchObjectId[CheckNode](n)) {
			c, err := g.createCheck(n)
			if err != nil {
				return nil, err
			}
			g.checks = append(g.checks, c)
		}
	}
	return g, nil
}

func (g *graph) createCheck(id database.ObjectId) (CheckNode, error) {
	c, err := g.scheme.CreateObject(id.GetType(), database.SetObjectNameFromId[CheckNode](id))
	if err == nil {
		if n, ok := id.(Node); ok {
			c.SetNode(n)
		}
	}
	return c, nil
}

func (g *graph) IsEmpty() bool {
	return len(g.nodes) == 0
}

func (g *graph) Objects() []database.ObjectId {
	return maputils.Keys(g.nodes, database.CompareObjectId)
}

func (g *graph) RootObjects() []database.ObjectId {
	return slices.Clone(g.roots)
}

func (g *graph) CheckObjects() []database.ObjectId {
	return sliceutils.Transform(g.checks, database.GetObjectId[CheckNode])
}

func (g *graph) rootObjects() []database.ObjectId {
	var ids []database.ObjectId
	if len(g.nodes) > 0 {
		leaves := g.EvaluatedGraph.Roots()
		ns := maputils.Keys(g.nodes)[0].GetNamespace()
		for _, l := range leaves {
			t := l.GetType()
			i := strings.Index(t, ":")
			e := g.mm.GetInternalType(t[:i]).Element(Phase(t[i+1:]))
			if e != nil {
				t := e.TriggeredBy()
				if t != nil {
					oid := database.NewObjectId(*t, ns, l.GetName())
					ids = append(ids, oid)
				}
			}
		}
		slices.SortFunc(ids, database.CompareObjectId)
	}
	return ids
}

func (g *graph) GetObject(id database.ObjectId) database.Object {
	n := g.nodes[database.NewObjectIdFor(id)]
	if n != nil {
		return n.Object()
	}
	return nil
}

func (g *graph) HasObject(id database.ObjectId) bool {
	return g.nodes[database.NewObjectIdFor(id)] != nil
}

func (g *graph) HasRootObject(id database.ObjectId) bool {
	return slices.ContainsFunc(g.roots, database.MatchObjectId[database.ObjectId](id))
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
	for _, n := range g.checks {
		id := database.NewObjectIdFor(n)
		o, err := odb.GetObject(n)
		if err != nil {
			return false, "", err
		}

		ok, status, err := n.DBCheck(log, g, o)
		if err != nil {
			return false, "", err
		}
		if ok && model.IsFinalStatus(status) {
			f := "processing"
			if final {
				f = "final"
			}
			rstatus = model.MergeStatus(rstatus, status)
			log.Info("- found status {{status}} for {{oid}} -> {{resulting}}/{{mode}}", "status", status, "oid", id, "resulting", rstatus, "mode", f)
		} else {
			final = false
			rstatus = model.MergeStatus(rstatus, status)
			log.Info("- {{oid}} still processing ({{status}} -> {{resulting}}/{{mode}}", "status", status, "oid", id, "resulting", rstatus, "mode", "processing")
		}
	}
	return final, rstatus, nil
}

func GraphId(typ string, name string, phase Phase) version.Id {
	// Graphs are always local to a namespace.
	return version.NewId(NewTypeId(typ, phase), name)
}

func (g *graph) MapToPhaseId(oid database.ObjectId) version.Id {
	t := g.mm.GetExternalType(oid.GetType())
	if t == nil {
		return nil
	}
	trigger := t.Trigger()
	if trigger == nil {
		return nil
	}
	return version.NewId(trigger.Id(), oid.GetName())
}

////////////////////////////////////////////////////////////////////////////////

type DefaultCheckNode[O database.Object] struct {
	database.ObjectMeta
	Configured O
}

func (c *DefaultCheckNode[O]) SetNode(n Node) {
	c.Configured = n.(O)
}
