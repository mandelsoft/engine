package processor

import (
	"errors"
	"fmt"
	"sync"

	. "github.com/mandelsoft/engine/pkg/processing/mmids"
	"github.com/mandelsoft/logging"

	"github.com/mandelsoft/engine/pkg/database"
	"github.com/mandelsoft/engine/pkg/processing/internal"
	"github.com/mandelsoft/engine/pkg/processing/metamodel"
	"github.com/mandelsoft/engine/pkg/processing/mmids"
	"github.com/mandelsoft/engine/pkg/processing/model"
	"github.com/mandelsoft/engine/pkg/processing/objectbase"
	"github.com/mandelsoft/engine/pkg/utils"
)

type processingModel struct {
	lock sync.Mutex

	m  model.Model
	mm metamodel.MetaModel
	ob objectbase.Objectbase

	namespaces map[string]*namespaceInfo
}

var _ ProcessingModel = (*processingModel)(nil)

func newProcessingModel(m model.Model) *processingModel {
	return &processingModel{
		m:          m,
		mm:         m.MetaModel(),
		ob:         m.Objectbase(),
		namespaces: map[string]*namespaceInfo{"": newNamespaceInfo(model.NewRootNamespace(m.MetaModel().NamespaceType()))},
	}
}

func (p *processingModel) ObjectBase() objectbase.Objectbase {
	return p.ob
}

func (p *processingModel) MetaModel() metamodel.MetaModel {
	return p.mm
}

func (p *processingModel) SchemeTypes() objectbase.SchemeTypes {
	return p.m.SchemeTypes()
}

func (m *processingModel) Namespaces() []string {
	m.lock.Lock()
	defer m.lock.Unlock()
	return utils.MapKeys(m.namespaces)
}

func (m *processingModel) GetNamespace(name string) Namespace {
	m.lock.Lock()
	defer m.lock.Unlock()
	return m.namespaces[name]
}

func (p *processingModel) GetElement(id ElementId) Element {
	p.lock.Lock()
	defer p.lock.Unlock()
	return p.getElement(id)
}

func (p *processingModel) _GetElement(id ElementId) _Element {
	p.lock.Lock()
	defer p.lock.Unlock()
	return p.getElement(id)
}

func (p *processingModel) getElement(id ElementId) _Element {
	ni := p.namespaces[id.GetNamespace()]
	if ni == nil {
		return nil
	}
	return ni._GetElement(id)
}

func (m *processingModel) AssureNamespace(log logging.Logger, name string, create bool) (*namespaceInfo, error) {
	m.lock.Lock()
	defer m.lock.Unlock()

	return m.assureNamespace(log, name, create)
}

func (m *processingModel) assureNamespace(log logging.Logger, name string, create bool) (*namespaceInfo, error) {
	ni := m.namespaces[name]
	if ni == nil {
		nns, nn := NamespaceId(name)
		b, err := m.ob.GetObject(database.NewObjectId(m.mm.NamespaceType(), nns, nn))
		if err != nil {
			if !errors.Is(err, database.ErrNotExist) || !create {
				log.Error("cannot get namespace object for {{namespace}}", "namespace", name)
				return nil, err
			}
			log.Info("creating namespace object for {{namespace}}", "namespace", name)
			b, err = m.ob.SchemeTypes().CreateObject(m.mm.NamespaceType(), objectbase.SetObjectName(nns, nn))
			if err != nil {
				log.Error("cannot create namespace object for {{namespace}}", "namespace", name)
				return nil, err
			}
		} else {
			log.Info("found namespace object for {{namespace}}", "namespace", name)
		}
		ni = newNamespaceInfo(b.(internal.NamespaceObject))
		m.namespaces[name] = ni
	}
	return ni, nil
}

func (m *processingModel) AssureElementObjectFor(log logging.Logger, e model.ExternalObject) (Element, error) {
	m.lock.Lock()
	defer m.lock.Unlock()

	t := m.mm.GetPhaseFor(e.GetType())
	if t == nil {
		return nil, NonTemporaryError(fmt.Errorf("external object type %q not configured", e.GetType()))
	}
	id := mmids.NewElementIdForObject(*t, e)

	ns, err := m.assureNamespace(log, e.GetNamespace(), true)
	if err != nil {
		return nil, err
	}

	var elem Element
	oid := id.ObjectId()
	i := ns.internal[oid]
	if i == nil {
		log.Info("creating internal object for {{extid}}")
		_i, err := m.ob.SchemeTypes().CreateObject(t.GetType(), objectbase.SetObjectName(id.GetNamespace(), id.GetName()))
		if err != nil {
			log.Error("creation of internal object for external object {{extid}} failed", "error", err)
			return nil, err
		}

		i = _i.(model.InternalObject)

		_, err = i.AddFinalizer(m.ob, FINALIZER)
		if err != nil {
			return nil, err
		}

		ns.internal[mmids.NewObjectIdFor(i)] = i
		for _, ph := range m.mm.GetInternalType(t.GetType()).Phases() {
			id := mmids.NewElementId(t.GetType(), e.GetNamespace(), e.GetName(), ph)
			pe := newElement(ph, i)
			ns.elements[id] = pe
			if ph == t.GetPhase() {
				elem = pe
			}
		}
	} else {
		elem = ns.elements[id]
	}
	if elem == nil {
		panic(fmt.Errorf("no elem found for %s", id))
	}
	return elem, nil
}

func (m *processingModel) lister() ObjectLister {
	return &watchEventLister{m}
}
