package database

type EventHandler interface {
	HandleEvent()
}
type Database interface {
	ListObjects(typ string, ns string) ([]Object, error)
	GetObject(ObjectId) (Object, error)
	SetObject(Object) error

	RegisterEventHandler(EventHandler, ns string, types ...string)
}
