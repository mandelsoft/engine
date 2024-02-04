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

type Database[O database.Object] struct {
	lock sync.Mutex
	_HandlerRegistry
	registry database.HandlerRegistry
	encoding database.Encoding[O]
	path     string
	fs       vfs.FileSystem
}

var _ database.Database[database.Object] = (*Database[database.Object])(nil)

func New[O database.Object](s database.Encoding[O], path string, fss ...vfs.FileSystem) (database.Database[O], error) {
	fs := utils.OptionalDefaulted(vfs.FileSystem(osfs.OsFs), fss...)

	err := fs.MkdirAll(path, 0o0700)
	if err != nil && !errors.Is(err, vfs.ErrExist) {
		return nil, err
	}

	d := &Database[O]{encoding: s, path: path, fs: fs}
	reg := database.NewHandlerRegistry(d)
	d._HandlerRegistry, d.registry = reg.(_HandlerRegistry), reg
	return d, nil
}

func (d *Database[O]) SchemeTypes() database.SchemeTypes[O] {
	return d.encoding
}

func (d *Database[O]) ListObjects(typ, ns string) ([]O, error) {
	d.lock.Lock()
	defer d.lock.Unlock()
	list, err := d.listObjectIds(typ, ns, ns == "")
	if err != nil {
		return nil, err
	}
	result := make([]O, len(list), len(list))
	for i, id := range list {
		o, err := d.get(id)
		if err != nil {
			return nil, err
		}
		result[i] = o
	}
	return result, err
}

func (d *Database[O]) ListObjectIds(typ, ns string, atomic ...func()) ([]database.ObjectId, error) {
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

func (d *Database[O]) listObjectIds(typ, ns string, closure bool) ([]database.ObjectId, error) {
	if ns == "" {
		return d.list(typ, ns, true, closure)
	} else {
		return d.list(typ, ns, false, closure)
	}
}

func (d *Database[O]) list(typ, ns string, dir, closure bool) ([]database.ObjectId, error) {
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

func (d *Database[O]) GetObject(id database.ObjectId) (O, error) {
	d.lock.Lock()
	defer d.lock.Unlock()
	return d.get(id)
}

func (d *Database[O]) get(id database.ObjectId) (O, error) {
	var _nil O

	path := d.OPath(id)
	data, err := vfs.ReadFile(d.fs, path)
	if err != nil {
		if errors.Is(err, vfs.ErrNotExist) {
			return _nil, database.ErrNotExist
		}
		return _nil, err
	}
	o, err := d.encoding.Decode(data)

	if err != nil {
		return _nil, err
	}

	if !database.EqualObjectId(o, id) {
		return _nil, fmt.Errorf("corrupted database: %s does not contain object with id %s", path, database.StringId(id))
	}
	return o, nil
}

func (d *Database[O]) SetObject(o O) error {
	path := d.OPath(o)

	err := d.fs.MkdirAll(filepath.Dir(path), 0o700)
	if err != nil {
		return err
	}

	d.lock.Lock()
	defer d.lock.Unlock()

	if g, ok := utils.TryCast[database.GenerationAccess](o); ok {
		gen := g.GetGeneration()
		old, err := d.get(o)
		if err != nil && !errors.Is(err, database.ErrNotExist) {
			return err
		}
		if err == nil {
			var ok bool
			og, ok := utils.TryCast[database.GenerationAccess](old)
			if !ok {
				return fmt.Errorf("incosistent types for read and write")
			}
			oldgen := og.GetGeneration()
			if gen >= 0 && gen != oldgen {
				return database.ErrModified
			}
			gen = oldgen
		}
		g.SetGeneration(gen + 1)
	}

	data, err := yaml.Marshal(o)
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

func (d *Database[O]) DeleteObject(id database.ObjectId) error {
	path := d.OPath(id)

	d.lock.Lock()
	defer d.lock.Unlock()

	if ok, err := vfs.Exists(d.fs, path); !ok && err == nil {
		return database.ErrNotExist
	}
	err := d.fs.Remove(path)
	if err != nil {
		return err
	}
	d.registry.TriggerEvent(id)
	return nil
}

func (d *Database[O]) Path(path string) string {
	return filepath.Join(d.path, path)
}

func (d *Database[O]) OPath(id database.ObjectId) string {
	return filepath.Join(d.path, Path(id))
}
