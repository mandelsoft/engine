package metamodel

import (
	"fmt"
	"slices"
)

func validateDep(in []InternalTypeSpecification, d DependencyType) error {
	if i := hasInt(in, d.Type); i == nil {
		return fmt.Errorf("uses non-existing internal type %q", d.Type)
	} else {
		if !slices.Contains(i.Phases, d.Phase) {
			return fmt.Errorf("uses non-existing phase %q internal type %q", d.Phase, d.Type)
		}
	}
	return nil
}

func Validate(m MetaModel) error {

	enc := m.GetEncoding()

	spec := m.GetSpecification()

	for _, e := range spec.ExternalTypes {
		err := validateDep(spec.InternalTypes, e.Trigger)
		if err != nil {
			return fmt.Errorf("external type %q: %w", e.Name, err)
		}
		if !enc.HasType(e.Name) {
			return fmt.Errorf("no encoding for external type %q", e.Name)
		}

		for _, d := range e.Dependencies {
			err := validateDep(spec.InternalTypes, d)
			if err != nil {
				return fmt.Errorf("dependency of external type %q: %w", e.Name, err)
			}
		}
	}
	for _, i := range spec.InternalTypes {
		if !enc.HasType(i.Name) {
			return fmt.Errorf("no encoding for internal type %q", i.Name)
		}

		o, err := enc.CreateObject(i.Name)
		if err != nil {
			return fmt.Errorf("error creating internal object %q: %w", i.Name, err)
		}
		if _, ok := o.(GenerationObject); !ok {
			return fmt.Errorf("internal object %q must support generations to detect modification race conditions", i.Name)
		}
	}

	if spec.NamespaceType == "" {
		return fmt.Errorf("no namespace type specified")
	}
	if !enc.HasType(spec.NamespaceType) {
		return fmt.Errorf("no encoding for namespace type %q", spec.NamespaceType)
	}
	ns, err := enc.CreateObject(spec.NamespaceType)
	if err != nil {
		return fmt.Errorf("error creating namespace object %q: %w", spec.NamespaceType, err)
	}
	if _, ok := ns.(Namespace); !ok {
		return fmt.Errorf("namespace type %q does not implement namespace interface", spec.NamespaceType)
	}
	return nil
}

func hasInt(in []InternalTypeSpecification, n string) *InternalTypeSpecification {
	for _, i := range in {
		if i.Name == n {
			return &i
		}
	}
	return nil
}

func hasExtt(ex []ExternalTypeSpecification, n string) *ExternalTypeSpecification {
	for _, e := range ex {
		if e.Name == n {
			return &e
		}
	}
	return nil
}
