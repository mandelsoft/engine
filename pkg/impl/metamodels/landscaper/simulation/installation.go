package simulation

import (
	"github.com/mandelsoft/engine/pkg/metamodels/landscaper"
)

type Installation struct {
	Object
	Dependencies
}

var _ landscaper.Installation = (*Installation)(nil)

func NewInstallation(name string) *Installation {
	return newVersionedObject[Installation](landscaper.TYPE_INSTALLATION, name)
}
