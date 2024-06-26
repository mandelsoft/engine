package db

import (
	. "github.com/mandelsoft/engine/pkg/processing/mmids"
	"github.com/mandelsoft/engine/pkg/processing/model"
	"github.com/mandelsoft/engine/pkg/processing/model/support/db"

	"github.com/mandelsoft/engine/pkg/database"
	mymetamodel "github.com/mandelsoft/engine/pkg/metamodels/valopdemo"
)

func init() {
	database.MustRegisterType[Value, db.Object](Scheme) // Goland requires second type parameter
}

type Value struct {
	db.ObjectMeta

	Spec   ValueSpec   `json:"spec"`
	Status ValueStatus `json:"status"`
}

var _ database.Object = (*Value)(nil)

func (n *Value) GetStatusValue() string {
	return string(n.Status.Status)
}

type ValueSpec struct {
	Provider string `json:"provider"`
	Value    int    `json:"value"`
}

type ValueStatus struct {
	Status           model.Status `json:"status,omitempty"`
	Message          string       `json:"message,omitempty"`
	RunId            RunId        `json:"runid,omitempty"`
	DetectedVersion  string       `json:"detectedVersion,omitempty"`
	ObservedVersion  string       `json:"observedVersion,omitempty"`
	EffectiveVersion string       `json:"effectiveVersion,omitempty"`

	Result *int `json:"result,omitempty"`
}

func NewValueNode(ns, n string, value int) *Value {
	return &Value{
		ObjectMeta: db.NewObjectMeta(mymetamodel.TYPE_VALUE, ns, n),
		Spec: ValueSpec{
			Value: value,
		},
	}
}

func NewResultNode(ns, n string, operator string) *Value {
	return &Value{
		ObjectMeta: db.NewObjectMeta(mymetamodel.TYPE_VALUE, ns, n),
		Spec: ValueSpec{
			Provider: operator,
		},
	}
}
