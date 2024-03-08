package main

import (
	"math/rand"
	"slices"
	"time"

	"github.com/goombaio/namegenerator"
	mymetamodel "github.com/mandelsoft/engine/pkg/metamodels/foreigndemo"
	"github.com/mandelsoft/engine/pkg/processing/metamodel"
	"github.com/mandelsoft/engine/pkg/processing/mmids"
	"github.com/mandelsoft/engine/pkg/processing/model"
	elemwatch "github.com/mandelsoft/engine/pkg/processing/watch"
	"github.com/mandelsoft/engine/pkg/utils"
)

var NS = "testspace"

var generator = namegenerator.NewNameGenerator(time.Now().UnixNano())
var mm metamodel.MetaModel

func init() {
	mm, _ = mymetamodel.NewMetaModel("demo")
}

func CreateEvents(objects *ObjectSpace) {
	for {
		mod := false
		i := rand.Intn(1000)
		switch {
		case i < 5:
			mod = CreateNamespace(objects)
		case i < 10:
			mod = DeleteNamespace(objects)
		case i < 50:
			mod = CreateObject(objects)
		case i < 100:
			mod = DeleteObject(objects)
		case i < 300:
			mod = LockGraph(objects)
		case i < 800:
			mod = Progress(objects)
		case i < 900:
			mod = AddLink(objects)
		case i < 1000:
			mod = RemoveLink(objects)
		}
		if mod {
			time.Sleep(time.Second)
		}
	}
}

func CreateNamespace(objects *ObjectSpace) bool {
	ns := objects.ChooseRandomNamespace()
	name := generator.Generate()

	id := elemwatch.Id{
		Kind:      mm.NamespaceType(),
		Namespace: NamespaceName(ns.Node),
		Name:      name,
		Phase:     "",
	}

	if objects.Has(id) {
		return false
	}
	node := &elemwatch.Event{
		Node:   id,
		Status: "Ready",
	}
	log.Debug("create namespace {{id}}", "id", id)
	objects.Set(node)
	return true
}

func DeleteNamespace(objects *ObjectSpace) bool {
	ns := objects.ChooseRandomNamespace()

	if ns.Node.Namespace == "" && (ns.Node.Name == NS || ns.Node.Name == "") {
		return false
	}
	if objects.IsUsed(ns.Node) {
		return false
	}
	log.Debug("delete namespace {{id}}", "id", ns.Node)
	objects.Delete(ns.Node)
	return true
}

func CreateObject(objects *ObjectSpace) bool {
	ns := objects.ChooseRandomNamespace()
	name := ""

	for name == "" {
		name = generator.Generate()
	}

	t := Random(mm.InternalTypes())
	p := Random(mm.Phases(t))

	id := elemwatch.Id{
		Kind:      t,
		Namespace: NamespaceName(ns.Node),
		Name:      name,
		Phase:     string(p),
	}

	if objects.Has(id) {
		return false
	}
	node := &elemwatch.Event{
		Node:   id,
		Status: "Initial",
	}
	log.Debug("create object {{id}}", "id", id)
	objects.Set(node)
	return true
}

func DeleteObject(objects *ObjectSpace) bool {
	o := objects.ChooseRandomObject()
	if o == nil || o.Node.Phase == "" {
		return false
	}
	if o.Lock != "" || objects.IsUsed(o.Node) {
		return false
	}
	log.Debug("delete object {{id}}", "id", o.Node)
	objects.Delete(o.Node)
	return true
}

func Progress(objects *ObjectSpace) bool {
	o := objects.ChooseRandomObject()
	if o == nil {
		return false
	}

	cur := model.Status(o.Status)

	if o.Lock == "" {
		return false
	}
	for _, l := range o.Links {
		if objects.Get(l).Lock != "" {
			return false
		}
	}

	s := Random(follow[model.Status(o.Status)])
	log.Debug("change status {{id}}", "id", o.Node, "status", s)
	o.Status = string(s)
	if cur == model.STATUS_COMPLETED || cur == model.STATUS_FAILED || cur == model.STATUS_INVALID {
		log.Debug("    -> unlock")
		o.Lock = ""
	}
	objects.Set(o)
	return true
}

func RemoveLink(objects *ObjectSpace) bool {
	o := objects.ChooseRandomObject()
	if o == nil {
		return false
	}
	if len(o.Links) == 0 {
		return false
	}

	i := rand.Intn(len(o.Links))
	log.Debug("remove link {{id}} -> {{link}}", "id", o.Node, "link", o.Links[i])
	o.Links = slices.Delete(o.Links, i, i+1)
	objects.Set(o)
	return true
}

func AddLink(objects *ObjectSpace) bool {
	o := objects.ChooseRandomObject()
	if o == nil {
		return false
	}
	t := mm.GetInternalType(o.GetType())
	if t == nil {
		return false
	}
	p := t.Element(mmids.Phase(o.Node.Phase))

	deps := p.Dependencies()
	if len(deps) == 0 {
		return false
	}

	d := Random(deps)

	list := objects.List(string(d.Id().GetType()), o.GetNamespace(), string(d.Id().GetPhase()))
	if len(list) == 0 {
		return false
	}

	l := Random(list)
	if objects.IsCycle(o, objects.Get(l)) {
		return false
	}

	o.Links = append(o.Links, l)
	log.Debug("add link {{id}} -> {{link}}", "id", o.Node, "link", l)
	objects.Set(o)
	return true
}

func LockGraph(objects *ObjectSpace) bool {
	o := objects.ChooseRandomObject()
	if o == nil {
		return false
	}
	t := mm.GetInternalType(o.GetType())
	if t == nil {
		return false
	}

	if o.Lock != "" {
		return false
	}
	runid := mmids.NewRunId()

	g := objects.GetGraph(o)
	for _, e := range g {
		if e.Lock != "" {
			log.Debug("  {{id}} already locked", "id", e.Node)
			return false
		}
	}
	log.Debug("lock graph {{id}}: {{elements}}",
		"id", o.Node,
		"elements", utils.TransformSlice(g, NodeId),
	)

	for _, e := range g {
		e.Lock = string(runid)
		objects.Set(e)
	}
	return true
}

func NodeId(e *elemwatch.Event) elemwatch.Id {
	return e.Node
}

func Random[E any](list []E) E {
	return list[rand.Intn(len(list))]
}

var follow = map[model.Status][]model.Status{
	model.Status("Ready"):  []model.Status{model.Status("Locked")},
	model.Status("Locked"): []model.Status{model.Status("Ready")},

	model.Status("Initial"): []model.Status{model.STATUS_INVALID, model.STATUS_PROCESSING, model.STATUS_BLOCKED},
	model.STATUS_INITIAL:    []model.Status{model.STATUS_INVALID, model.STATUS_PROCESSING, model.STATUS_BLOCKED},
	model.STATUS_INVALID:    []model.Status{model.STATUS_PROCESSING, model.STATUS_BLOCKED},
	model.STATUS_FAILED:     []model.Status{model.STATUS_INVALID, model.STATUS_PROCESSING, model.STATUS_BLOCKED},
	model.STATUS_BLOCKED:    []model.Status{model.STATUS_INVALID, model.STATUS_PROCESSING},
	model.STATUS_PROCESSING: []model.Status{model.STATUS_FAILED, model.STATUS_COMPLETED},
	model.STATUS_COMPLETED:  []model.Status{model.STATUS_INVALID, model.STATUS_PROCESSING, model.STATUS_BLOCKED},
}
