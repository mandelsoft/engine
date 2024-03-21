package support

import (
	"reflect"
	"sync"

	"github.com/mandelsoft/engine/pkg/database"
	"github.com/mandelsoft/engine/pkg/processing/model"
	"github.com/mandelsoft/engine/pkg/processing/model/support/db"
	"github.com/mandelsoft/engine/pkg/processing/objectbase"
	"github.com/mandelsoft/goutils/generics"
)

type UpdateRequest struct {
	Lock sync.Mutex
	Wrapper
}

var _ objectbase.Object = (*UpdateRequest)(nil)
var _ model.UpdateRequestObject = (*UpdateRequest)(nil)

func (n *UpdateRequest) GetDatabase(ob objectbase.Objectbase) database.Database[db.Object] {
	return objectbase.GetDatabase[db.Object](ob)
}

func (n *UpdateRequest) GetAction() *model.UpdateAction {
	return generics.Cast[db.DBUpdateRequest](n.GetBase()).GetAction()
}

func (n *UpdateRequest) SetAction(ob objectbase.Objectbase, a *model.UpdateAction) (bool, error) {
	n.Lock.Lock()
	defer n.Lock.Unlock()

	dbo := n.GetDatabase(ob)
	mod := func(o db.Object) (bool, bool) {
		ur := generics.Cast[db.DBUpdateRequest](o)
		b := ur.GetAction()
		if reflect.DeepEqual(a, b) {
			return false, false
		}
		ur.SetAction(a)
		return true, true
	}

	o := n.GetBase()
	r, err := database.Modify(dbo, &o, mod)
	n.SetBase(o)
	return r, err
}

func (n *UpdateRequest) GetStatus() *model.UpdateStatus {
	return generics.Cast[db.DBUpdateRequest](n.GetBase()).GetStatus()
}

func (n *UpdateRequest) SetStatus(ob objectbase.Objectbase, a *model.UpdateStatus) (bool, error) {
	n.Lock.Lock()
	defer n.Lock.Unlock()

	dbo := n.GetDatabase(ob)
	mod := func(o db.Object) (bool, bool) {
		ur := generics.Cast[db.DBUpdateRequest](o)
		b := ur.GetStatus()
		if reflect.DeepEqual(a, b) {
			return false, false
		}
		ur.SetStatus(a)
		return true, true
	}

	o := n.GetBase()
	r, err := database.Modify(dbo, &o, mod)
	n.SetBase(o)
	return r, err
}
