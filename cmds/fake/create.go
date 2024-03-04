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

	var id elemwatch.Id

	if ns.Node.Name == NS {
		if rand.Intn(100) < 10 {
			id = elemwatch.Id{
				Kind:  mm.NamespaceType(),
				Name:  name,
				Phase: "",
			}
		} else {
			id = elemwatch.Id{
				Kind:      mm.NamespaceType(),
				Namespace: NamespaceName(ns.Node),
				Name:      name,
				Phase:     "",
			}
		}
	}

	if objects.Has(id) {
		return false
	}
	node := &elemwatch.Event{
		Node:   id,
		Status: "Ready",
	}
	log.Debug("{{id}} create namespace", "id", id)
	objects.Set(node)
	return true
}

func DeleteNamespace(objects *ObjectSpace) bool {
	ns := objects.ChooseRandomNamespace()

	if ns.Node.Namespace == "" && ns.Node.Name == NS {
		return false
	}
	if objects.IsUsed(ns.Node) {
		return false
	}
	log.Debug("{{id}} delete namespace", "id", ns.Node)
	objects.Delete(ns.Node)
	return true
}

func CreateObject(objects *ObjectSpace) bool {
	ns := objects.ChooseRandomNamespace()
	name := generator.Generate()

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
	log.Debug("{{id}} create", "id", id)
	objects.Set(node)
	return true
}

func DeleteObject(objects *ObjectSpace) bool {
	o := objects.ChooseRandomObject()
	if o == nil || o.Node.Phase == "" {
		return false
	}
	if objects.IsUsed(o.Node) {
		return false
	}
	log.Debug("{{id}} delete", "id", o.Node)
	objects.Delete(o.Node)
	return true
}

func Progress(objects *ObjectSpace) bool {
	o := objects.ChooseRandomObject()
	if o == nil {
		return false
	}
	s := Random(follow[model.Status(o.Status)])
	log.Debug("{{id}} change status", "id", o.Node, "status", s)
	o.Status = string(s)
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
	log.Debug("{{id}} removing link {{link}}", "id", o.Node, "link", o.Links[i])
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
	o.Links = append(o.Links, l)
	log.Debug("{{id}} adding link {{link}}", "id", o.Node, "link", l)
	objects.Set(o)
	return true
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
