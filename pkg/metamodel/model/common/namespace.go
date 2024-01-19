package common

type Namespace interface {
	Object

	GetNamespaceName() string

	GetLock() RunId
	TryLock(db Objectbase, id RunId) (bool, error)
}
