package common

type NamespaceObject interface {
	Object

	GetNamespaceName() string

	GetLock() RunId

	ClearLock(ob Objectbase, id RunId) (bool, error)
	TryLock(db Objectbase, id RunId) (bool, error)
}
