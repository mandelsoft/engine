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

type Database struct {
	lock   sync.Mutex
	scheme database.Scheme
	path   string
	fs     vfs.FileSystem
}

var _ database.Database = (*Database)(nil)

func New(s database.Scheme, path string, fss ...vfs.FileSystem) (database.Database, error) {
	fs := utils.OptionalDefaulted(vfs.FileSystem(osfs.OsFs), fss...)

	err := fs.MkdirAll(path, 0o0700)
	if err != nil && !errors.Is(err, vfs.ErrExist) {
		return nil, err
	}

	return &Database{scheme: s, path: path, fs: fs}, nil
}

func (d *Database) ListObjects(typ, ns string) ([]database.Object, error) {
	d.lock.Lock()
	defer d.lock.Unlock()
	return d.listObjects(typ, ns, ns == "")
}

func (d *Database) listObjects(typ, ns string, closure bool) ([]database.Object, error) {
	if ns == "" {
		return d.list(typ, ns, true, closure)
	} else {
		return d.list(typ, ns, false, closure)
	}
}

func (d *Database) list(typ, ns string, dir, closure bool) ([]database.Object, error) {
	var result []database.Object

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
				o, err := d.get(id)
				if err != nil {
					return nil, err
				}
				result = append(result, o)
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
	return vfs.WriteFile(d.fs, path, data, 0o600)
}

func (d Database) RegisterEventHandler(EventHandler, ns string, types ...string) {
	// TODO implement me
	panic("implement me")
}

func (d *Database) Path(path string) string {
	return filepath.Join(d.path, path)
}

func (d *Database) OPath(id database.ObjectId) string {
	return filepath.Join(d.path, Path(id))
}
