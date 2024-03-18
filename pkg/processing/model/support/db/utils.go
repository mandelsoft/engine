package db

import (
	"github.com/mandelsoft/engine/pkg/database"
	"github.com/mandelsoft/goutils/generics"
)

func AddFinalizer[O database.Object](odb database.Database[Object], o *O, f string) (bool, error) {
	return database.Modify(odb, o, func(o O) (bool, bool) {
		m := generics.Cast[Object](o).AddFinalizer(f)
		return m, m
	})
}

func RemoveFinalizer[O database.Object](odb database.Database[Object], o *O, f string) (bool, error) {
	return database.Modify(odb, o, func(o O) (bool, bool) {
		m := generics.Cast[Object](o).RemoveFinalizer(f)
		return m, m
	})
}
