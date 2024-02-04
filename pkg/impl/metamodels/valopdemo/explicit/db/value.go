package db

import (
	"github.com/mandelsoft/engine/pkg/database"
	"github.com/mandelsoft/engine/pkg/metamodel/common"
	"github.com/mandelsoft/engine/pkg/metamodel/model"
	"github.com/mandelsoft/engine/pkg/metamodel/model/support"

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
	Owner string `json:"owner"`
	Value int    `json:"value"`
}

type ValueStatus struct {
	Status           common.ProcessingStatus `json:"status,omitempty"`
	Message          string                  `json:"message,omitempty"`
	RunId            model.RunId             `json:"runid,omitempty"`
	DetectedVersion  string                  `json:"detectedVersion,omitempty"`
	ObservedVersion  string                  `json:"observedVersion,omitempty"`
	EffectiveVersion string                  `json:"effectiveVersion,omitempty"`

	Result *int `json:"result,omitempty"`
}

func NewValueNode(ns, n string, value int) *Value {
	return &Value{
		GenerationObjectMeta: database.NewGenerationObjectMeta(mymetamodel.TYPE_VALUE, ns, n),
		Spec: ValueSpec{
			Value: value,
		},
	}
}

func NewResultNode(ns, n string, operator string) *Value {
	return &Value{
		GenerationObjectMeta: database.NewGenerationObjectMeta(mymetamodel.TYPE_VALUE, ns, n),
		Spec: ValueSpec{
			Owner: operator,
		},
	}
}
