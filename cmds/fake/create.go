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
		i := rand.Intn(100)
		switch {
		case i < 5:
			mod = CreateObject(objects)
		case i < 10:
			mod = DeleteObject(objects)
		case i < 80:
			mod = Progress(objects)
		case i < 90:
			mod = AddLink(objects)
		case i < 100:
			mod = RemoveLink(objects)
		}
		if mod {
			time.Sleep(time.Second)
		}
	}
}

func CreateObject(objects *ObjectSpace) bool {
	name := generator.Generate()

	t := Random(mm.InternalTypes())
	p := Random(mm.Phases(t))

	id := elemwatch.Id{
		Kind:      t,
		Namespace: NS,
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
	if o == nil {
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
	p := t.Element(mmids.Phase(o.Node.Phase))

	deps := p.Dependencies()
	if len(deps) == 0 {
		return false
	}

	d := Random(deps)

	list := objects.List(string(d.Id().GetType()), NS, string(d.Id().GetPhase()))
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
	model.Status("Initial"): []model.Status{model.STATUS_INVALID, model.STATUS_PROCESSING, model.STATUS_BLOCKED},
	model.STATUS_INITIAL:    []model.Status{model.STATUS_INVALID, model.STATUS_PROCESSING, model.STATUS_BLOCKED},
	model.STATUS_INVALID:    []model.Status{model.STATUS_PROCESSING, model.STATUS_BLOCKED},
	model.STATUS_FAILED:     []model.Status{model.STATUS_INVALID, model.STATUS_PROCESSING, model.STATUS_BLOCKED},
	model.STATUS_BLOCKED:    []model.Status{model.STATUS_INVALID, model.STATUS_PROCESSING},
	model.STATUS_PROCESSING: []model.Status{model.STATUS_FAILED, model.STATUS_COMPLETED},
	model.STATUS_COMPLETED:  []model.Status{model.STATUS_INVALID, model.STATUS_PROCESSING, model.STATUS_BLOCKED},
}
