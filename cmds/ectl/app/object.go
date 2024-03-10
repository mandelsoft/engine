package app

import (
	"github.com/mandelsoft/engine/pkg/processing/model/support/db"
)

type Object = *db.Unstructured

type List struct {
	Items []Object `json:"items"`
}
