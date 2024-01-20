package hashmapped

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/mandelsoft/engine/pkg/database"
	"github.com/mandelsoft/engine/pkg/metamodel/model/objectbase"
	"github.com/mandelsoft/engine/pkg/runtime"
	"github.com/mandelsoft/engine/pkg/wrapper"
)

type IdMapping[S DBObject] struct {
	db database.Database[S]
}

var _ wrapper.IdMapping[DBObject] = (*IdMapping[DBObject])(nil)

func (m *IdMapping[S]) Namespace(ns string) string {
	basens, _ := BaseNamespace(ns)
	return basens
}

func (m *IdMapping[S]) Inbound(id wrapper.ObjectId) wrapper.ObjectId {
	ns, logns := BaseNamespace(id.GetNamespace())

	b := fmt.Sprintf("%s/%s", logns, id.GetName())
	h := sha256.Sum256([]byte(b))
	return database.NewObjectId(id.GetType(), ns, hex.EncodeToString(h[:]))
}

func (m *IdMapping[S]) Outbound(id wrapper.ObjectId) wrapper.ObjectId {
	o, err := m.db.GetObject(id)
	if err != nil {
		return database.NewObjectId(id.GetType(), "", "")
	}
	return database.NewObjectId(id.GetType(), o.GetEffectiveNamespace(), o.GetEffectiveName())
}

func (m *IdMapping[S]) OutboundObject(o S) wrapper.ObjectId {
	return database.NewObjectId(o.GetType(), o.GetEffectiveNamespace(), o.GetEffectiveName())
}

type DBObject interface {
	database.Object

	GetEffectiveName() string
	GetEffectiveNamespace() string
}

type Object[S DBObject] interface {
	wrapper.Object[S]
	objectbase.Object
}

// NewObjectbase provides a new object base with hashed hierarchical namespaces
// and functional wrappers (W).
func NewObjectbase[W Object[S], S DBObject](db database.Database[S], types runtime.SchemeTypes[W]) (objectbase.Objectbase, error) {
	return wrapper.NewDatabase[objectbase.Object, W, S](db, types, &IdMapping[S]{})
}

func BaseNamespace(ns string) (string, string) {
	i := strings.Index(ns, "/")
	if i < 0 {
		return ns, ""
	}
	return ns[:i], ns[i+1:]
}
