package model

import (
	"fmt"

	"github.com/mandelsoft/engine/pkg/metamodel"
	common2 "github.com/mandelsoft/engine/pkg/metamodel/common"
	"github.com/mandelsoft/engine/pkg/metamodel/objectbase"
	"github.com/mandelsoft/engine/pkg/utils"
)

type ModelSpecification struct {
	Name       string
	MetaModel  metamodel.MetaModelSpecification
	Objectbase objectbase.Specification
}

func NewModelSpecification(name string, spec metamodel.MetaModelSpecification, specification objectbase.Specification) ModelSpecification {
	return ModelSpecification{name, spec, specification}
}

func (s *ModelSpecification) GetMetaModel() (metamodel.MetaModel, error) {
	m, err := metamodel.NewMetaModel(s.Name, s.MetaModel)
	if err != nil {
		return nil, err
	}
	return m, nil
}

func (s *ModelSpecification) Validate() error {
	enc := s.Objectbase.SchemeTypes()

	m, err := metamodel.NewMetaModel(s.Name, s.MetaModel)
	if err != nil {
		return err
	}
	for _, n := range m.ExternalTypes() {
		o, err := enc.CreateObject(n)
		if err != nil {
			return fmt.Errorf("error creating external object %q: %w", n, err)
		}
		if _, ok := o.(common2.ExternalObject); !ok {
			return fmt.Errorf("external object %q must support model interface for external objects %s", n, utils.TypeOf[common2.ExternalObject]())
		}
	}
	for _, n := range m.InternalTypes() {
		o, err := enc.CreateObject(n)
		if err != nil {
			return fmt.Errorf("error creating internal object %q: %w", n, err)
		}
		if _, ok := o.(common2.InternalObject); !ok {
			return fmt.Errorf("internal object %q must support model interface for internal objects %s", n, utils.TypeOf[common2.InternalObject]())
		}
	}

	if m.NamespaceType() == "" {
		return fmt.Errorf("no namespace type specified")
	}
	if !enc.HasType(m.NamespaceType()) {
		return fmt.Errorf("no encoding for namespace type %q", m.NamespaceType())
	}
	ns, err := enc.CreateObject(m.NamespaceType())
	if err != nil {
		return fmt.Errorf("error creating namespace object %q: %w", m.NamespaceType(), err)
	}
	if _, ok := ns.(common2.Namespace); !ok {
		return fmt.Errorf("namespace type %q does not implement namespace interface", m.NamespaceType())
	}
	return nil
}

func hasInt(in []metamodel.InternalTypeSpecification, n string) *metamodel.InternalTypeSpecification {
	for _, i := range in {
		if i.Name == n {
			return &i
		}
	}
	return nil
}

func hasExt(ex []metamodel.ExternalTypeSpecification, n string) *metamodel.ExternalTypeSpecification {
	for _, e := range ex {
		if e.Name == n {
			return &e
		}
	}
	return nil
}
