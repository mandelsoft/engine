package metamodel

import (
	"slices"

	"github.com/mandelsoft/engine/pkg/metamodel/model/common"
)

type TypeSpecification struct {
	Name string
}

type PhaseSpecification struct {
	Name         common.Phase
	Dependencies []DependencyTypeSpecification
}

type InternalTypeSpecification struct {
	TypeSpecification
	Phases []PhaseSpecification
}

func PhaseSpec(name common.Phase, deps ...DependencyTypeSpecification) PhaseSpecification {
	return PhaseSpecification{
		Name:         name,
		Dependencies: slices.Clone(deps),
	}
}

func IntSpec(tname string, phase PhaseSpecification, phases ...PhaseSpecification) InternalTypeSpecification {
	phases = append([]PhaseSpecification{phase}, phases...)

	return InternalTypeSpecification{
		TypeSpecification: TypeSpecification{tname},
		Phases:            phases,
	}
}

type DependencyTypeSpecification struct {
	Type  string
	Phase common.Phase
}

func Dep(typ string, phase common.Phase) DependencyTypeSpecification {
	return DependencyTypeSpecification{typ, phase}
}

type ExternalTypeSpecification struct {
	TypeSpecification

	Trigger DependencyTypeSpecification
}

func ExtSpec(tname string, inttype string, phase common.Phase) ExternalTypeSpecification {
	if phase == "" {
		phase = DEFAULT_PHASE
	}
	return ExternalTypeSpecification{
		TypeSpecification: TypeSpecification{tname},
		Trigger: DependencyTypeSpecification{
			Type:  inttype,
			Phase: phase,
		},
	}
}

type MetaModelSpecification struct {
	NamespaceType string

	ExternalTypes []ExternalTypeSpecification
	InternalTypes []InternalTypeSpecification
}
