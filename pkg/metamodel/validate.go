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

	ex, in := m.GetTypes()

	for _, e := range ex {
		err := validateDep(in, e.Trigger)
		if err != nil {
			return fmt.Errorf("external type %q: %w", e.Name, err)
		}
		if !enc.HasType(e.Name) {
			return fmt.Errorf("no encoding for external type %q", e.Name)
		}

		for _, d := range e.Dependencies {
			err := validateDep(in, d)
			if err != nil {
				return fmt.Errorf("dependency of external type %q: %w", e.Name, err)
			}
		}
	}
	for _, i := range in {
		if !enc.HasType(i.Name) {
			return fmt.Errorf("no encoding for internal type %q", i.Name)
		}
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
