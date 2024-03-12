package scenario1

import (
	"time"

	"github.com/mandelsoft/engine/cmds/fake/objectspace"
	mymetamodel "github.com/mandelsoft/engine/pkg/metamodels/foreigndemo"
	"github.com/mandelsoft/engine/pkg/processing/mmids"
	elemwatch "github.com/mandelsoft/engine/pkg/processing/watch"
)

func Scenario(objects *objectspace.ObjectSpace) {
	CreateNamespace(objects, "", "demo")
	CreateObject(objects, mymetamodel.TYPE_VALUE_STATE, "demo", "A", mymetamodel.PHASE_PROPAGATE)
	CreateObject(objects, mymetamodel.TYPE_VALUE_STATE, "demo", "B", mymetamodel.PHASE_PROPAGATE)

	time.Sleep(time.Minute)
	DeleteObject(objects, mymetamodel.TYPE_VALUE_STATE, "demo", "A", mymetamodel.PHASE_PROPAGATE)
}

func CreateNamespace(objects *objectspace.ObjectSpace, ns, name string) {
	id := elemwatch.Id{
		Kind:      objectspace.MetaModel.NamespaceType(),
		Namespace: ns,
		Name:      name,
		Phase:     "",
	}

	node := &elemwatch.Event{
		Node:   id,
		Status: "Ready",
	}
	objectspace.Log.Debug("create namespace {{id}}", "id", id)
	objects.Set(node)
}

func CreateObject(objects *objectspace.ObjectSpace, typ, ns, name string, phase mmids.Phase) {
	id := elemwatch.Id{
		Kind:      typ,
		Namespace: ns,
		Name:      name,
		Phase:     string(phase),
	}

	node := &elemwatch.Event{
		Node:   id,
		Status: "Completed",
	}
	objectspace.Log.Debug("create object {{id}}", "id", id)
	objects.Set(node)
}

func DeleteObject(objects *objectspace.ObjectSpace, typ, ns, name string, phase mmids.Phase) bool {
	id := elemwatch.Id{
		Kind:      typ,
		Namespace: ns,
		Name:      name,
		Phase:     string(phase),
	}

	objectspace.Log.Debug("delete object {{id}}", "id", id)
	objects.Delete(id)
	return true
}
