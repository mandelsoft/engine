package database

type RunId string

type Object interface {
	ObjectId
}

type ObjectId interface {
	GetName() string
	GetType() string
}

type objectid struct {
	typ  string
	name string
}

func (o *objectid) GetName() string {
	return o.name
}

func (o *objectid) GetType() string {
	return o.typ
}

func NewObjectId(typ, name string) ObjectId {
	return &objectid{typ, name}
}

func NewObjectIdFor(id ObjectId) ObjectId {
	return &objectid{
		typ:  id.GetType(),
		name: id.GetName(),
	}
}
