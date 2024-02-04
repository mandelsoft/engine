package metamodel

import (
	"slices"

	. "github.com/mandelsoft/engine/pkg/processing/mmids"
)

type TypeSpecification struct {
	Name string
}

type PhaseSpecification struct {
	Name         Phase
	Dependencies []DependencyTypeSpecification
}

type InternalTypeSpecification struct {
	TypeSpecification
	Phases []PhaseSpecification
}

func PhaseSpec(name Phase, deps ...DependencyTypeSpecification) PhaseSpecification {
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
	Phase Phase
}

func Dep(typ string, phase Phase) DependencyTypeSpecification {
	return DependencyTypeSpecification{typ, phase}
}

type ExternalTypeSpecification struct {
	TypeSpecification

	// Trigger describes the type/phase which should be triggered on state change.
	Trigger DependencyTypeSpecification
	// ForeignControlled indicates that the object is controlled by another
	// controller. Its state therefore describes the actual external status
	// provided by this controller and not teh object specification.
	// This object MUST provide information, whether the status reflects the
	// requested target state described by the object specification.
	// This will be evaluated by the implementation of the internal object
	// implementing the triggered phase.
	ForeignControlled bool
}

func ExtSpec(tname string, inttype string, phase Phase) ExternalTypeSpecification {
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
