package db

import (
	"github.com/mandelsoft/engine/pkg/processing/model"
)

type DBUpdateRequest interface {
	Object

	GetAction() *model.UpdateAction
	SetAction(*model.UpdateAction)

	GetStatus() *model.UpdateStatus
	SetStatus(*model.UpdateStatus)
}
