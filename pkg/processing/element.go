package processing

import (
	"fmt"

	"github.com/mandelsoft/engine/pkg/metamodel/model/common"
)

type Element struct {
	phase  common.Phase
	object common.Object

	runid common.RunId

	current common.State
	target  common.State
}

var _ common.Element = (*Element)(nil)

func NewElement(phase common.Phase, obj common.Object) *Element {
	return &Element{
		phase:  phase,
		object: obj,
	}
}

func (e *Element) GetNamespace() string {
	return e.object.GetNamespace()
}

func (e *Element) GetName() string {
	return fmt.Sprintf("%s/%s/%s", e.object.GetType(), e.object.GetName(), e.phase)
}

func (e Element) GetPhase() common.Phase {
	return e.phase
}

func (e *Element) GetObject() common.Object {
	return e.object
}

func (e Element) GetCurrentState() common.State {
	return e.current
}

func (e Element) GetTargetState() common.State {
	return e.target
}
