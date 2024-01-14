package filesystem

import (
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"

	"github.com/mandelsoft/engine/pkg/database"
	"github.com/mandelsoft/engine/pkg/utils"
	"github.com/mandelsoft/vfs/pkg/osfs"
	"github.com/mandelsoft/vfs/pkg/vfs"
	"sigs.k8s.io/yaml"
)

type _HandlerRegistry = database.HandlerRegistrationTest

type Database struct {
	lock sync.Mutex
	_HandlerRegistry
	registry database.HandlerRegistry
	scheme   database.Scheme
	path     string
	fs       vfs.FileSystem
}

var _ database.Database = (*Database)(nil)

func New(s database.Scheme, path string, fss ...vfs.FileSystem) (database.Database, error) {
	fs := utils.OptionalDefaulted(vfs.FileSystem(osfs.OsFs), fss...)

	err := fs.MkdirAll(path, 0o0700)
	if err != nil && !errors.Is(err, vfs.ErrExist) {
		return nil, err
	}

	d := &Database{scheme: s, path: path, fs: fs}
	reg := database.NewHandlerRegistry(d)
	d._HandlerRegistry, d.registry = reg.(_HandlerRegistry), reg
	return d, nil
}

func (d *Database) ListObjects(typ, ns string) ([]database.Object, error) {
	d.lock.Lock()
	defer d.lock.Unlock()
	list, err := d.listObjectIds(typ, ns, ns == "")
	if err != nil {
		return nil, err
	}
	result := make([]database.Object, len(list), len(list))
	for i, id := range list {
		o, err := d.get(id)
		if err != nil {
			return nil, err
		}
		result[i] = o
	}
	return result, err
}

func (d *Database) ListObjectIds(typ, ns string, atomic ...func()) ([]database.ObjectId, error) {
	d.lock.Lock()
	defer d.lock.Unlock()
	list, err := d.listObjectIds(typ, ns, ns == "")
	if err == nil {
		for _, a := range atomic {
			a()
		}
	}
	return list, err
}

func (d *Database) listObjectIds(typ, ns string, closure bool) ([]database.ObjectId, error) {
	if ns == "" {
		return d.list(typ, ns, true, closure)
	} else {
		return d.list(typ, ns, false, closure)
	}
}

func (d *Database) list(typ, ns string, dir, closure bool) ([]database.ObjectId, error) {
	var result []database.ObjectId

	list, err := vfs.ReadDir(d.fs, d.Path(filepath.Join(typ, ns)))
	if err != nil {
		if errors.Is(err, vfs.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	for _, e := range list {
		if e.IsDir() {
			if dir || closure {
				r, err := d.list(typ, filepath.Join(ns, e.Name()), false, closure)
				if err != nil {
					return nil, err
				}
				result = append(result, r...)
			}
		} else {
			if !dir && strings.HasSuffix(e.Name(), ".yaml") {
				id := database.NewObjectId(typ, ns, e.Name()[:len(e.Name())-5])
				result = append(result, id)
			}
		}
	}
	return result, nil
}

func (d *Database) GetObject(id database.ObjectId) (database.Object, error) {
	d.lock.Lock()
	defer d.lock.Unlock()
	return d.get(id)
}

func (d *Database) get(id database.Object) (database.Object, error) {
	path := d.OPath(id)
	data, err := vfs.ReadFile(d.fs, path)
	if err != nil {
		return nil, err
	}
	o, err := d.scheme.Decode(data)

	if err != nil {
		return nil, err
	}

	if !database.EqualObjectId(o, id) {
		return nil, fmt.Errorf("corrupted database: %s does not contain object with id %s", path, database.StringId(id))
	}
	return o, nil
}

func (d *Database) SetObject(o database.Object) error {
	path := d.OPath(o)

	data, err := yaml.Marshal(o)
	if err != nil {
		return err
	}
	err = d.fs.MkdirAll(filepath.Dir(path), 0o700)
	if err != nil {
		return err
	}
	err = vfs.WriteFile(d.fs, path, data, 0o600)
	if err != nil {
		d.fs.Remove(path)
		return err
	}
	d.registry.TriggerEvent(o)
	return nil
}

func (d *Database) Path(path string) string {
	return filepath.Join(d.path, path)
}

func (d *Database) OPath(id database.ObjectId) string {
	return filepath.Join(d.path, Path(id))
}
