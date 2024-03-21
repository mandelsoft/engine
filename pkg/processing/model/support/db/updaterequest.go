package db

import (
	"slices"

	"github.com/mandelsoft/engine/pkg/processing/model"
	"github.com/mandelsoft/goutils/generics"
)

type UpdateRequest struct {
	ObjectMeta `json:",inline"`

	Spec   model.UpdateAction `json:"spec"`
	Status model.UpdateStatus `json:"status"`
}

var (
	_ DBUpdateRequest = (*UpdateRequest)(nil)
)

func (u *UpdateRequest) GetStatusValue() string {
	return u.Status.Status
}

func (u *UpdateRequest) GetAction() *model.UpdateAction {
	return u.Spec.Copy()
}

func (u *UpdateRequest) SetAction(action *model.UpdateAction) {
	u.Spec = *action.Copy()
}

func (u *UpdateRequest) GetStatus() *model.UpdateStatus {
	return generics.Pointer(u.Status)
}

func (u *UpdateRequest) SetStatus(status *model.UpdateStatus) {
	u.Status = *status
}

func (u *UpdateRequest) RequestAction(a string, refs ...model.ElementRef) *UpdateRequest {
	u.Spec.Action = a
	u.Spec.Elements = slices.Clone(refs)
	return u
}
