package database

type EventHandler interface {
	HandleEvent(id ObjectId)
}

type ObjectLister interface {
	ListObjectIds(typ string, ns string, atomic ...func()) ([]ObjectId, error)
}

type Database interface {
	HandlerRegistration
	ObjectLister
	ListObjects(typ string, ns string) ([]Object, error)

	GetObject(ObjectId) (Object, error)
	SetObject(Object) error
}
