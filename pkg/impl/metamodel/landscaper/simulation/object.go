package simulation

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"sync"

	"github.com/mandelsoft/engine/pkg/database"
	"github.com/mandelsoft/engine/pkg/metamodel/landscaper"
)

type ObjectId struct {
	Type string `json:"type"`
	Name string `json:"name"`
}

var _ database.ObjectId = (*ObjectId)(nil)

func NewObjectId(typ, name string) *ObjectId {
	return &ObjectId{
		Type: typ,
		Name: name,
	}
}

func NewObjectIdFor(id database.ObjectId) *ObjectId {
	return &ObjectId{
		Type: id.GetType(),
		Name: id.GetName(),
	}
}

func (o *ObjectId) GetType() string {
	return o.Type
}

func (o *ObjectId) GetName() string {
	return o.Name
}

////////////////////////////////////////////////////////////////////////////////

type Object struct {
	lock sync.Mutex
	ObjectId
}

var _ database.Object = (*Object)(nil)

func NewObject(typ, name string) Object {
	return Object{
		ObjectId: ObjectId{
			Type: typ,
			Name: name,
		},
	}
}

func NewObjectForId(id database.ObjectId) Object {
	return Object{
		ObjectId: ObjectId{
			Type: id.GetType(),
			Name: id.GetName(),
		},
	}
}

////////////////////////////////////////////////////////////////////////////////

type Dependencies struct {
	Links []ObjectId
	self  interface{}
}

var _ landscaper.Dependencies = (*Dependencies)(nil)

func NewDependencies(self interface{}) Dependencies {
	return Dependencies{self: self}
}

func (d *Dependencies) setSelf(s interface{}) {
	d.self = s
}

func (d *Dependencies) GetLinks() []database.ObjectId {
	r := make([]database.ObjectId, len(d.Links))
	for i := range d.Links {
		e := d.Links[i]
		r[i] = &e
	}
	return r
}

func (d *Dependencies) GetVersion() string {
	data, err := json.Marshal(d.self)
	if err != nil {
		panic(err)
	}
	b := sha256.Sum256(data)
	return hex.EncodeToString(b[:])
}

func (d *Dependencies) AddDep(id database.ObjectId) {
	i := NewObjectIdFor(id)
	for _, e := range d.Links {
		if e == *i {
			return
		}
	}
	d.Links = append(d.Links, *i)
	return
}

type dependencies[P any] interface {
	database.Object
	landscaper.Dependencies

	setSelf(o interface{})
	*P
}

func newVersionedObject[T any, P dependencies[T]](typ, name string) P {
	var o T
	p := (P)(&o)
	p.setSelf(p)
	return p
}

////////////////////////////////////////////////////////////////////////////////

type InternalObject[E landscaper.ExternalObject] struct {
	Object
	Dependencies

	LockOwner database.RunId `json:"lock"`

	ActualVersion string `json:"actualVersion"`
	TargetVersion string `json:"targetVersion"`
	TargetState   json.RawMessage
}

var _ landscaper.InternalObject[landscaper.ExternalObject] = (*InternalObject[landscaper.ExternalObject])(nil)

func (i *InternalObject[E]) Lock(id database.RunId) (bool, error) {
	i.lock.Lock()
	defer i.lock.Unlock()

	if i.LockOwner != "" {
		return false, nil
	}
	i.LockOwner = id
	return true, nil
}

func (i *InternalObject[E]) Unlock() error {
	i.lock.Lock()
	defer i.lock.Unlock()

	i.LockOwner = ""
	return nil
}

func (i *InternalObject[E]) GetActualVersion() string {
	return i.ActualVersion
}

func (i *InternalObject[E]) GetTargetVersion() string {
	return i.TargetVersion
}

func (i *InternalObject[E]) SetActualVersion(v string) {
	i.ActualVersion = v
}

func (i *InternalObject[E]) SetTargetVersion(v string) {
	i.TargetVersion = v
}

func (i *InternalObject[E]) SetTargetState(e E) error {
	data, err := json.Marshal(e)
	if err != nil {
		return err
	}
	i.TargetState = data
	return nil
}
