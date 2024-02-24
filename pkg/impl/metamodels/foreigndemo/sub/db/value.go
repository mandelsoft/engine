package db

import (
	mymetamodel "github.com/mandelsoft/engine/pkg/metamodels/foreigndemo"
	. "github.com/mandelsoft/engine/pkg/processing/mmids"
	"github.com/mandelsoft/engine/pkg/processing/model/support/db"

	"github.com/mandelsoft/engine/pkg/database"
	"github.com/mandelsoft/engine/pkg/processing/model"
)

func init() {
	database.MustRegisterType[Value, db.DBObject](Scheme) // Goland requires second type parameter
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
		ObjectMeta: db.NewObjectMeta(mymetamodel.TYPE_VALUE, ns, n),
		Spec: ValueSpec{
			Value: value,
		},
	}
}
