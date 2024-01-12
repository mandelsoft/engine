package simulation

import (
	"github.com/mandelsoft/engine/pkg/database"
)

type Installation struct {
	Object
	Dependencies
}

var _ database.Installation = (*Installation)(nil)

func NewInstallation(name string) *Installation {
	return newVersionedObject[Installation](database.TYPE_INSTALLATION, name)
}
