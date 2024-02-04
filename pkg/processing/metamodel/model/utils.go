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
