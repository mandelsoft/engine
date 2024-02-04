package hashmapped

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/mandelsoft/engine/pkg/database"
	"github.com/mandelsoft/engine/pkg/database/wrapper"
	"github.com/mandelsoft/engine/pkg/metamodel/common"
	"github.com/mandelsoft/engine/pkg/runtime"
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
	common.Object
}

// NewObjectbase provides a new object base with hashed hierarchical namespaces
// and functional wrappers (W).
func NewObjectbase[W Object[S], S DBObject](db database.Database[S], types runtime.SchemeTypes[W]) (common.Objectbase, error) {
	odb, err := wrapper.NewDatabase[common.Object, W, S](db, types, &IdMapping[S]{})
	if err != nil {
		return nil, err
	}
	return &objectbase{odb}, nil
}

type objectbase struct {
	database.Database[common.Object]
}

func (d *objectbase) CreateObject(id common.ObjectId) (common.Object, error) {
	return d.SchemeTypes().CreateObject(id.GetType(), SetObjectName(id.GetNamespace(), id.GetName()))
}

func BaseNamespace(ns string) (string, string) {
	i := strings.Index(ns, "/")
	if i < 0 {
		return ns, ""
	}
	return ns[:i], ns[i+1:]
}
