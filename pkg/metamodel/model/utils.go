package model

import (
	"github.com/mandelsoft/engine/pkg/metamodel/common"
)

func NewObjectIdForType(t string, s common.NameSource) ObjectId {
	return common.NewObjectId(t, s.GetNamespace(), s.GetName())
}

func NewElementIdForType(t string, s common.NameSource, phase Phase) ElementId {
	return common.NewElementId(t, s.GetNamespace(), s.GetName(), phase)
}
