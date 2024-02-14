package model

import (
	"github.com/mandelsoft/engine/pkg/processing/mmids"
)

func NewObjectIdForType(t string, s mmids.NameSource) mmids.ObjectId {
	return mmids.NewObjectId(t, s.GetNamespace(), s.GetName())
}

func NewElementIdForType(t string, s mmids.NameSource, phase mmids.Phase) mmids.ElementId {
	return mmids.NewElementId(t, s.GetNamespace(), s.GetName(), phase)
}

func PhaseId(id mmids.ElementId, phase mmids.Phase) mmids.ElementId {
	return NewElementIdForType(id.GetType(), id, phase)
}

func SlaveId(id mmids.ElementId, t string, phase mmids.Phase) mmids.ElementId {
	return mmids.NewElementId(t, id.GetNamespace(), id.GetName(), phase)
}

func NamedSlaveId(id mmids.ElementId, t string, name string, phase mmids.Phase) mmids.ElementId {
	return mmids.NewElementId(t, id.GetNamespace(), name, phase)
}
