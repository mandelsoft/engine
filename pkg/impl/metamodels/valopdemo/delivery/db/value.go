package db

import (
	. "github.com/mandelsoft/engine/pkg/processing/mmids"

	"github.com/mandelsoft/engine/pkg/database"
	"github.com/mandelsoft/engine/pkg/processing/model"
	"github.com/mandelsoft/engine/pkg/processing/model/support"

	mymetamodel "github.com/mandelsoft/engine/pkg/metamodels/valopdemo"
)

func init() {
	database.MustRegisterType[Value, support.DBObject](Scheme) // Goland requires second type parameter
}

type Value struct {
	database.GenerationObjectMeta

	Spec   ValueSpec   `json:"spec"`
	Status ValueStatus `json:"status"`
}

var _ database.Object = (*Value)(nil)

type ValueSpec struct {
	Value int `json:"value"`
}

type ValueStatus struct {
	// ValueStateSpec is the local object state part held in the state object
	// and propagated as part of the status in the external object.
	ValueStateSpec `json:",inline"`

	Status           model.Status `json:"status,omitempty"`
	Message          string       `json:"message,omitempty"`
	RunId            RunId        `json:"runid,omitempty"`
	DetectedVersion  string       `json:"detectedVersion,omitempty"`
	ObservedVersion  string       `json:"observedVersion,omitempty"`
	EffectiveVersion string       `json:"effectiveVersion,omitempty"`
}

func NewValueNode(ns, n string, value int) *Value {
	return &Value{
		GenerationObjectMeta: database.NewGenerationObjectMeta(mymetamodel.TYPE_VALUE, ns, n),
		Spec: ValueSpec{
			Value: value,
		},
	}
}
