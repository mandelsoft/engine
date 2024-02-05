package db

import (
	. "github.com/mandelsoft/engine/pkg/processing/mmids"

	"github.com/mandelsoft/engine/pkg/database"
	"github.com/mandelsoft/engine/pkg/processing/model/support"
)

func init() {
	database.MustRegisterType[ValueState, support.DBObject](Scheme) // Goland requires second type parameter
}

type ValueState struct {
	support.InternalDBObjectSupport `json:",inline"`

	// Spec is the part of the object state held exclusively in the state object and not
	// on the external object. (there it is found as status)
	Spec ValueStateSpec `json:"spec,omitempty"`

	Current ValueCurrentState `json:"current"`
	Target  *ValueTargetState `json:"target,omitempty"`
}

var _ support.InternalDBObject = (*ValueState)(nil)

type ValueStateSpec struct {
	Provider string `json:"provider,omitempty"`
}

type ValueCurrentState struct {
	Provider string `json:"provider,omitempty"`

	InputVersion  string      `json:"inputVersion"`
	ObjectVersion string      `json:"objectVersion"`
	OutputVersion string      `json:"outputVersion"`
	Output        ValueOutput `json:"output"`
}

type ValueOutput struct {
	Origin *ObjectId `json:"origin,omitempty"`
	Value  int       `json:"value"`
}

type ValueTargetState struct {
	ObjectVersion string             `json:"version"`
	Spec          EffectiveValueSpec `json:"spec"`
}

// EffectiveValueSpec bundles the external spec
// with the internal spec consisting of
// the provider field.
// This field is NOT a spec field for the
// external object and kept for the internal object,
// only.
type EffectiveValueSpec struct {
	*ValueSpec     `json:",inline"`
	ValueStateSpec `json:",inline"`
}
