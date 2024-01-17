package metamodel

import (
	"fmt"

	"github.com/mandelsoft/engine/pkg/metamodel/model/common"
	"github.com/mandelsoft/engine/pkg/utils"
)

type MetaModel interface {
	Name() string

	NamespaceType() string
	InternalTypes() []string
	ExternalTypes() []string
}

type metaModel struct {
	name     string
	elements map[string]ElementType

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
		elements:  map[string]ElementType{},
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
			m.elements[e.Name()] = e
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

func (m *metaModel) ExternalTypes() []string {
	return utils.OrderedMapKeys(m.external)
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
