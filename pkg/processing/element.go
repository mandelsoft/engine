package processing

import (
	"fmt"

	"github.com/mandelsoft/engine/pkg/metamodel/model/common"
)

type Element interface {
	common.Element
}

type element struct {
	phase  common.Phase
	object common.InternalObject

	runid common.RunId

	current State
	target  State
}

var _ Element = (*element)(nil)

func NewElement(phase common.Phase, obj common.InternalObject) *element {
	return &element{
		phase:  phase,
		object: obj,
	}
}

func (e *element) GetNamespace() string {
	return e.object.GetNamespace()
}

func (e *element) GetName() string {
	return fmt.Sprintf("%s/%s/%s", e.object.GetType(), e.object.GetName(), e.phase)
}

func (e element) GetPhase() common.Phase {
	return e.phase
}

func (e *element) GetObject() common.InternalObject {
	return e.object
}

func (e element) GetCurrentState() State {
	return e.current
}

func (e element) GetTargetState() State {
	return e.target
}
