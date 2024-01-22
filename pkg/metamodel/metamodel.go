package metamodel

import (
	"fmt"
	"io"
	"slices"
	"sort"

	"github.com/mandelsoft/engine/pkg/metamodel/model/common"
	"github.com/mandelsoft/engine/pkg/utils"
)

type MetaModel interface {
	Name() string

	NamespaceType() string
	InternalTypes() []string
	Phases(objtype string) []Phase
	ExternalTypes() []string
	ElementTypes() []TypeId

	GetExternalType(name string) ExternalObjectType
	GetInternalType(name string) InternalObjectType

	GetPhaseFor(ext string) *TypeId
	GetTriggerFor(id TypeId) []string

	Dump(w io.Writer)
}

type metaModel struct {
	name     string
	elements map[TypeId]ElementType

	internal  map[string]*intDef
	external  map[string]ExternalObjectType
	namespace string
}

var _ (MetaModel) = (*metaModel)(nil)

type intDef struct {
	intType InternalObjectType
	phases  map[common.Phase]ElementType
}

func NewMetaModel(name string, spec MetaModelSpecification) (MetaModel, error) {
	m := &metaModel{
		name:      name,
		elements:  map[TypeId]ElementType{},
		internal:  map[string]*intDef{},
		external:  map[string]ExternalObjectType{},
		namespace: spec.NamespaceType,
	}

	for _, i := range spec.InternalTypes {
		def := &intDef{
			phases: map[common.Phase]ElementType{},
		}

		for _, p := range i.Phases {
			e := newElementType(i.Name, p.Name)
			m.elements[e.id] = e
			def.phases[p.Name] = e
		}
		def.intType = newInternalObjectType(i.Name, def.phases)
		m.internal[i.Name] = def
	}

	for _, i := range spec.InternalTypes {
		for _, p := range i.Phases {
			e := m.internal[i.Name].phases[p.Name]
			for _, d := range p.Dependencies {
				t, err := m.checkDep(d)
				if err != nil {
					return nil, fmt.Errorf("dependency \"%s:%s\" of phase %q of internal type %q: %w",
						d.Type, d.Phase, p.Name, i.Name, err)
				}
				e.addDependency(t)
			}
		}
	}

	for _, e := range spec.ExternalTypes {
		d := e.Trigger

		t, err := m.checkDep(d)
		if err != nil {
			return nil, fmt.Errorf("trigger \"%s:%s\" of external type %q: %w",
				d.Type, d.Phase, e.Name, err)
		}
		m.external[e.Name] = newExternalObjectType(e.Name, t)
	}

	return m, nil
}

func (m *metaModel) Name() string {
	return m.name
}

func (m *metaModel) NamespaceType() string {
	return m.namespace
}

func (m *metaModel) InternalTypes() []string {
	return utils.OrderedMapKeys(m.internal)
}

func (m *metaModel) Phases(objtype string) []Phase {
	i := m.internal[objtype]
	if i == nil {
		return nil
	}
	return utils.OrderedMapKeys(i.phases)
}

func (m *metaModel) ExternalTypes() []string {
	return utils.OrderedMapKeys(m.external)
}

func (m *metaModel) ElementTypes() []TypeId {
	list := utils.MapKeys(m.elements)

	slices.SortFunc(list, utils.CompareStringable[TypeId])
	return list
}

func (m *metaModel) GetExternalType(name string) ExternalObjectType {
	return m.external[name]
}

func (m *metaModel) GetInternalType(name string) InternalObjectType {
	d := m.internal[name]
	if d == nil {
		return nil
	}
	return d.intType
}

func (m *metaModel) GetPhaseFor(ext string) *TypeId {
	i := m.external[ext]
	if i == nil {
		return nil
	}
	return utils.Pointer(i.Trigger().Id())
}

func (m *metaModel) checkDep(d DependencyTypeSpecification) (ElementType, error) {
	ti := m.internal[d.Type]
	if ti == nil {
		return nil, fmt.Errorf("type %q not defined", d.Type)
	}
	t := ti.phases[d.Phase]
	if ti == nil {
		return nil, fmt.Errorf("phase %q not defined for type %q", d.Phase, d.Type)
	}
	return t, nil
}

func (m *metaModel) GetTriggerFor(id TypeId) []string {
	var r []string
	for _, e := range m.external {
		if e.Trigger().Id() == id {
			r = append(r, e.Name())
		}
	}
	sort.Strings(r)
	return r
}

func (m *metaModel) Dump(w io.Writer) {
	fmt.Fprintf(w, "Namespace type: %s\n", m.namespace)
	fmt.Fprintf(w, "External types:\n")
	for _, n := range m.ExternalTypes() {
		i := m.external[n]
		fmt.Fprintf(w, "- %s  (-> %s)\n", n, i.Trigger().Id())
		fmt.Fprintf(w, "  internal type: %s\n", i.Trigger().Id().Type())
		fmt.Fprintf(w, "  phase:         %s\n", i.Trigger().Id().Phase())
	}
	fmt.Fprintf(w, "Internal types:\n")
	for _, n := range m.InternalTypes() {
		i := m.internal[n]
		fmt.Fprintf(w, "- %s\n", n)
		fmt.Fprintf(w, "  phases:\n")
		for _, p := range i.intType.Phases() {
			fmt.Fprintf(w, "  - %s\n", p)
		}
	}
	fmt.Fprintf(w, "Element types:\n")
	for _, n := range m.ElementTypes() {
		i := m.elements[n]
		fmt.Fprintf(w, "- %s\n", n)
		fmt.Fprintf(w, "  dependencies:\n")
		for _, d := range i.Dependencies() {
			fmt.Fprintf(w, "  - %s\n", d.Id())
		}
	}
}
