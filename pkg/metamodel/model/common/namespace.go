package common

type Namespace interface {
	Object

	TryLock(db Objectbase, id RunId) (bool, error)
}
