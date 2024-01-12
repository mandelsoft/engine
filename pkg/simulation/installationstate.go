package simulation

import (
	"github.com/mandelsoft/engine/pkg/database"
)

type InstallationState struct {
	InternalObject[database.Installation] `json:",inline"`
}

var _ database.InstallationState = (*InstallationState)(nil)

func NewInstallationState(name string) *InstallationState {
	return newVersionedObject[InstallationState](database.TYPE_INSTALLATION_STATE, name)
}
