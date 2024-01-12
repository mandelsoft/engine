package database

type EventHandler interface {
	HandleEvent()
}
type Database interface {
	ListObjects(typ string, ns string) ([]Object, error)

	ListDataObjectStates(ns string) ([]DataObjectState, error)
	ListInstallationStates(ns string) ([]InstallationState, error)
	ListExecutionStates(ns string) ([]ExecutionState, error)

	ListNamespaces(ns string) ([]Namespace, error)

	RegisterEventHandler(EventHandler, ns string, types ...string)
	
	GetObject(ObjectId) Object

	GetDataObject(name string) (DataObject, error)
	GetInstallation(name string) (Installation, error)
	GetExecution(name string) (Execution, error)

	GetDataObjectState(name string) (DataObjectState, error)
	GetInstallationState(name string) (InstallationState, error)
	GetExecutionState(name string) (ExecutionState, error)

	GetNamespace(name string) (Namespace, error)
}
