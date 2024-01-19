package hashmapped

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/mandelsoft/engine/pkg/database"
	"github.com/mandelsoft/engine/pkg/metamodel/model/common"
	"github.com/mandelsoft/engine/pkg/metamodel/model/objectbase"
	"github.com/mandelsoft/engine/pkg/runtime"
	"github.com/mandelsoft/engine/pkg/utils"
)

type Object interface {
	database.Object

	GetEffectiveName() string
	GetEffectiveNamespace() string
}

type ModelObject[O Object] interface {
	common.Object

	SetBaseObject(O)
	GetBaseObject() O
}

type Objectbase[O Object] struct {
	db     database.Database[O]
	create database.SchemeTypes[ModelObject[O]]
	types  objectbase.SchemeTypes

	events database.HandlerRegistry
}

var _ objectbase.Objectbase = (*Objectbase[Object])(nil)

func NewObjectNase[O Object](db database.Database[O], enc database.Encoding[ModelObject[O]]) (objectbase.Objectbase, error) {
	types, err := runtime.ConvertTypes[objectbase.Object, ModelObject[O]](enc)
	if err != nil {
		return nil, err
	}
	r := &Objectbase[O]{
		db:     db,
		create: enc,
		types:  types.(objectbase.SchemeTypes), // required by Goland
	}
	events := database.NewHandlerRegistry(r)
	r.events = events

	db.RegisterHandler(&handler[O]{r}, false, "")
	return r, nil
}

type handler[O Object] struct {
	ob *Objectbase[O]
}

func (h *handler[O]) HandleEvent(id database.ObjectId) {
	o, err := h.ob.db.GetObject(id)
	if err != nil {
		return
	}
	effid := database.NewObjectId(id.GetType(), o.GetEffectiveNamespace(), o.GetEffectiveName())
	h.ob.events.TriggerEvent(effid)
}

func (o *Objectbase[O]) SchemeTypes() objectbase.SchemeTypes {
	return o.types
}

func (o *Objectbase[O]) RegisterHandler(h database.EventHandler, current bool, kind string, nss ...string) utils.Sync {
	return o.events.RegisterHandler(h, current, kind, nss...)
}

func (o *Objectbase[O]) UnregisterHandler(h database.EventHandler, kind string, nss ...string) {
	o.events.UnregisterHandler(h, kind, nss...)
}

func (o *Objectbase[O]) ListObjectIds(typ string, ns string, atomic ...func()) ([]database.ObjectId, error) {

	basens, _ := BaseNamespace(ns)
	list, err := o.db.ListObjectIds(typ, basens, atomic...)
	if err != nil {
		return nil, err
	}
	r := []database.ObjectId{}
	for _, id := range list {
		b, err := o.db.GetObject(id)
		if err != nil {
			return nil, err
		}
		if b.GetEffectiveNamespace() == ns {
			r = append(r, database.NewObjectId(typ, ns, b.GetEffectiveName()))
		}
	}
	return r, nil
}

func (o *Objectbase[O]) ListObjects(typ string, ns string) ([]objectbase.Object, error) {
	basens, _ := BaseNamespace(ns)
	list, err := o.db.ListObjects(typ, basens)
	if err != nil {
		return nil, err
	}
	r := []objectbase.Object{}
	for _, b := range list {
		if b.GetEffectiveNamespace() == ns {
			e, err := o.create.CreateObject(typ)
			if err != nil {
				return nil, err
			}
			e.SetBaseObject(b)
			r = append(r, e)
		}
	}
	return r, nil
}

func (o *Objectbase[O]) GetObject(id database.ObjectId) (objectbase.Object, error) {
	basens, basen := BaseName(id.GetNamespace(), id.GetName())
	b, err := o.db.GetObject(database.NewObjectId(id.GetType(), basens, basen))
	if err != nil {
		return nil, err
	}
	e, err := o.create.CreateObject(id.GetType())
	if err != nil {
		return nil, err
	}
	e.SetBaseObject(b)
	return e, nil
}

func (o *Objectbase[O]) SetObject(obj objectbase.Object) error {
	e, ok := obj.(ModelObject[O])
	if !ok {
		return fmt.Errorf("object %T not from object base", obj)
	}
	b := e.GetBaseObject()
	return o.db.SetObject(b)
}

func BaseNamespace(ns string) (string, string) {
	i := strings.Index(ns, "/")
	if i < 0 {
		return ns, ""
	}
	return ns[:i], ns[i+1:]
}

func BaseName(ns, name string) (string, string) {
	ns, logns := BaseNamespace(ns)

	b := fmt.Sprintf("%s/%s", logns, name)
	h := sha256.Sum256([]byte(b))
	return ns, hex.EncodeToString(h[:])
}
