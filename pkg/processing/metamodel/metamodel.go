package metamodel

import (
	"fmt"
	"io"
	"slices"
	"sort"

	. "github.com/mandelsoft/engine/pkg/processing/mmids"
	"k8s.io/apimachinery/pkg/util/sets"

	"github.com/mandelsoft/engine/pkg/utils"
)

type metaModel struct {
	name     string
	elements map[TypeId]*elementType

	internal  map[string]*intDef
	external  map[string]*externalObjectType
	namespace string
}

var _ MetaModel = (*metaModel)(nil)

type intDef struct {
	intType  *internalObjectType
	extTypes []string
	phases   map[Phase]*elementType
}

func NewMetaModel(name string, spec MetaModelSpecification) (MetaModel, error) {
	m := &metaModel{
		name:      name,
		elements:  map[TypeId]*elementType{},
		internal:  map[string]*intDef{},
		external:  map[string]*externalObjectType{},
		namespace: spec.NamespaceType,
	}

	for _, i := range spec.InternalTypes {
		def := &intDef{
			phases: map[Phase]*elementType{},
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
		if t.TriggeredBy() != nil {
			return nil, fmt.Errorf("trigger \"%s:%s\" of external type %q: already triggered by %q",
				d.Type, d.Phase, e.Name, *t.TriggeredBy())
		}
		t.setTrigger(e.Name)
		m.external[e.Name] = newExternalObjectType(e.Name, t, e.ForeignControlled)
	}

	for _, i := range m.internal {
		for _, p := range i.phases {
			if t := p.TriggeredBy(); t != nil {
				if !slices.Contains(i.extTypes, *t) {
					i.extTypes = append(i.extTypes, *t)
				}
			} else {
				found := false
				for _, d := range p.dependencies {
					if d.Id().GetType() == i.intType.Name() {
						found = true
						break
					}
				}
				if !found {
					return nil, fmt.Errorf("root phase %q internal type %q not triggered by any external type",
						p.id.GetPhase(), p.id.GetType())
				}
			}
			if c := cycle(p); c != nil {
				return nil, fmt.Errorf("phase cycle for internal type %q: %s",
					p.id.GetType(), utils.Join(c, "->"))
			}
			determineExtStates(p)
		}

		sort.Strings(i.extTypes)
		if len(i.extTypes) == 0 {
			return nil, fmt.Errorf("no trigger for any phase of internal type %q",
				i.intType.Name())
		}
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

func (m *metaModel) IsExternalType(name string) bool {
	return m.external[name] != nil
}

func (m *metaModel) IsForeignControlled(name string) bool {
	t := m.external[name]
	if t == nil {
		return false
	}
	return t.IsForeignControlled()
}

func (m *metaModel) GetExternalType(name string) ExternalObjectType {
	return _externalObjectType(m.external[name])
}

func (m *metaModel) GetDependentTypePhases(name TypeId) ([]Phase, []Phase) {
	d := m.internal[name.GetType()]
	if d == nil {
		return nil, nil
	}

	r := []Phase{name.GetPhase()}
	leafs := sets.Set[Phase]{}
	leafs.Insert(name.GetPhase())

	for i := 0; i < len(r); i++ {
		t := NewTypeId(name.GetType(), r[i])
		for _, ph := range d.intType.Phases() {
			if !slices.Contains(r, ph) && d.intType.Element(ph).HasDependency(t) {
				r = append(r, ph)
				leafs.Insert(ph)
				leafs.Delete(t.GetPhase())
			}
		}
	}

	return r, utils.OrderedMapKeys(leafs)
}

func (m *metaModel) IsInternalType(name string) bool {
	return m.internal[name] != nil
}

func (m *metaModel) GetInternalType(name string) InternalObjectType {
	d := m.internal[name]
	if d == nil {
		return nil
	}
	return d.intType
}

func (m *metaModel) HasElementType(name TypeId) bool {
	d := m.internal[name.GetType()]
	if d == nil {
		return false
	}
	return d.phases[name.GetPhase()] != nil
}

func (m *metaModel) GetElementType(name TypeId) ElementType {
	d := m.internal[name.GetType()]
	if d == nil {
		return nil
	}
	return d.phases[name.GetPhase()]
}

func (m *metaModel) HasDependency(s, d TypeId) bool {
	src := m.GetElementType(s)
	if src == nil {
		return false
	}
	return src.HasDependency(d)
}

func (m *metaModel) GetPhaseFor(ext string) *TypeId {
	i := m.external[ext]
	if i == nil {
		return nil
	}
	return utils.Pointer(i.Trigger().Id())
}

func (m *metaModel) checkDep(d DependencyTypeSpecification) (*elementType, error) {
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

func (m *metaModel) GetExternalTypesFor(id TypeId) []string {
	e := m.elements[id]
	if e == nil {
		return nil
	}
	return e.ExternalStates()
}

func (m *metaModel) GetTriggerTypeForElementType(id TypeId) *string {
	e := m.elements[id]
	if e == nil {
		return nil
	}
	return e.TriggeredBy()
}

func (m *metaModel) GetTriggeringTypesForInternalType(name string) []string {
	e := m.internal[name]
	if e == nil {
		return nil
	}
	return slices.Clone(e.extTypes)
}

func (m *metaModel) Dump(w io.Writer) {
	fmt.Fprintf(w, "Namespace type: %s\n", m.namespace)
	fmt.Fprintf(w, "External types:\n")
	for _, n := range m.ExternalTypes() {
		i := m.external[n]
		fmt.Fprintf(w, "- %s  (-> %s)\n", n, i.Trigger().Id())
		fmt.Fprintf(w, "  internal type: %s\n", i.Trigger().Id().GetType())
		fmt.Fprintf(w, "  phase:         %s\n", i.Trigger().Id().GetPhase())
	}
	fmt.Fprintf(w, "Internal types:\n")
	for _, n := range m.InternalTypes() {
		i := m.internal[n]
		fmt.Fprintf(w, "- %s\n", n)
		fmt.Fprintf(w, "  phases:\n")
		for _, p := range i.intType.Phases() {
			fmt.Fprintf(w, "  - %s\n", p)
		}
		fmt.Fprintf(w, "  trigger types:\n")
		for _, p := range i.extTypes {
			fmt.Fprintf(w, "  - %s\n", p)
		}
	}
	fmt.Fprintf(w, "Element types:\n")
	for _, n := range m.ElementTypes() {
		i := m.elements[n]
		fmt.Fprintf(w, "- %s\n", n)
		if t := i.TriggeredBy(); t != nil {
			fmt.Fprintf(w, "  triggered by: %s\n", *t)
		}
		fmt.Fprintf(w, "  dependencies:\n")
		for _, d := range i.Dependencies() {
			fmt.Fprintf(w, "  - %s\n", d.Id())
		}
		fmt.Fprintf(w, "  updated states:\n")
		for _, d := range i.ExternalStates() {
			fmt.Fprintf(w, "  - %s\n", d)
		}
	}
}

func cycle(p *elementType, stack ...Phase) []Phase {
	if c := utils.Cycle(p.id.GetPhase(), stack...); c != nil {
		if c[0] != p.id.GetPhase() {
			return c
		}
		return nil
	}
	for _, d := range p.dependencies {
		if d.id.GetType() != p.id.GetType() {
			continue
		}
		if d == p {
			continue
		}
		c := cycle(d, append(stack, p.id.GetPhase())...)
		if c != nil {
			return c
		}
	}
	return nil
}

func determineExtStates(p *elementType, stack ...Phase) []string {
	if p.states == nil {
		if p.trigger != nil {
			p.states = []string{*p.trigger}
		}
		for _, d := range p.dependencies {
			if d.id.GetType() != p.id.GetType() {
				continue
			}
			if d == p {
				continue
			}
			p.states = utils.AppendUnique(p.states, determineExtStates(d, append(stack, p.id.GetPhase())...)...)
		}
		sort.Strings(p.states)
	}
	return p.states
}
