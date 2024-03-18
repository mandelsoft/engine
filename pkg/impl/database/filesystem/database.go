package filesystem

import (
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"

	"github.com/mandelsoft/engine/pkg/database"
	"github.com/mandelsoft/goutils/general"
	"github.com/mandelsoft/goutils/generics"
	"github.com/mandelsoft/logging"
	"github.com/mandelsoft/vfs/pkg/osfs"
	"github.com/mandelsoft/vfs/pkg/vfs"
	"sigs.k8s.io/yaml"
)

type _HandlerRegistry database.HandlerRegistrationTest

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
	fs := general.OptionalDefaulted(vfs.FileSystem(osfs.OsFs), fss...)

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

func (d *Database[O]) ListObjects(typ string, closure bool, ns string) ([]O, error) {
	d.lock.Lock()
	defer d.lock.Unlock()

	list, err := d.listObjectIds(typ, closure, ns)
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

func (d *Database[O]) ListObjectIds(typ string, closure bool, ns string, atomic ...func()) ([]database.ObjectId, error) {
	d.lock.Lock()
	defer d.lock.Unlock()
	list, err := d.listObjectIds(typ, closure, ns)
	if err == nil {
		for _, a := range atomic {
			a()
		}
	}
	return list, err
}

func (d *Database[O]) listObjectIds(typ string, closure bool, ns string) ([]database.ObjectId, error) {
	if typ != "" && !CheckType(typ) {
		return nil, fmt.Errorf("invalid type %q", typ)
	}
	if ns == "/" {
		ns = ""
	}
	if !CheckNamespace(ns) {
		return nil, fmt.Errorf("invalid namespace %q", typ)
	}
	return d.list(typ, ns, false, closure)
}

func (d *Database[O]) list(typ, ns string, dir, closure bool) ([]database.ObjectId, error) {
	var result []database.ObjectId

	var types []string

	if typ == "" {
		list, err := vfs.ReadDir(d.fs, d.path)
		if err != nil {
			if errors.Is(err, vfs.ErrNotExist) {
				return nil, nil
			}
			return nil, err
		}
		for _, n := range list {
			if n.IsDir() {
				types = append(types, n.Name())
			}
		}
	} else {
		types = []string{typ}
	}

	for _, typ := range types {
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
	}
	return result, nil
}

func (d *Database[O]) GetObject(id database.ObjectId) (O, error) {
	var _nil O
	if !CheckId(id) {
		return _nil, fmt.Errorf("invalid id %q", id)
	}

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
		return _nil, fmt.Errorf("object %s: %w", database.StringId(id), err)
	}

	if !database.EqualObjectId(o, id) {
		return _nil, fmt.Errorf("corrupted database: %s does not contain object with id %s", path, database.StringId(id))
	}
	return o, nil
}

func (d *Database[O]) SetObject(o O) error {
	if !CheckId(o) {
		return fmt.Errorf("invalid id %q", database.NewObjectIdFor(o))
	}

	path := d.OPath(o)

	log := logging.DefaultContext().Logger(REALM)
	log.Debug("set object", "path", path)
	err := d.fs.MkdirAll(filepath.Dir(path), 0o700)
	if err != nil {
		log.LogError(err, "cannot create folder", "path", filepath.Dir(path))
		return err
	}

	d.lock.Lock()
	defer func() {
		if err == nil {
			// trigger must be called outside of lock
			d.registry.TriggerEvent(o)
		}
	}()
	defer d.lock.Unlock()

	err = d._doSetObject(log, path, o)
	return err
}

func (d *Database[O]) _doSetObject(log logging.Logger, path string, o O) error {
	old, err := d.get(o)
	if err != nil && !errors.Is(err, database.ErrNotExist) {
		log.LogError(err, "cannot read old file", "path", path)
		return err
	}

	if g, ok := generics.TryCast[database.GenerationAccess](o); ok {
		gen := g.GetGeneration()
		if err == nil {
			var ok bool
			og, ok := generics.TryCast[database.GenerationAccess](old)
			if !ok {
				log.Error("inconsistent types for read and write", "path", path)
				return fmt.Errorf("inconsistent types for read and write")
			}
			oldgen := og.GetGeneration()
			if gen >= 0 && gen != oldgen {
				return database.ErrModified
			}
			gen = oldgen
		}
		g.SetGeneration(gen + 1)
	}

	if f, ok := generics.TryCast[database.Finalizable](o); ok {
		if err == nil {
			f.PreserveDeletion(generics.Cast[database.Finalizable](old).GetDeletionInfo())
		}
		if f.IsDeleting() && len(f.GetFinalizers()) == 0 {
			_, err := d._doDeleteObject(log, path, o)
			return err
		}
	}

	data, err := yaml.Marshal(o)
	if err != nil {
		log.LogError(err, "cannot marshal content", "path", path)
		return err
	}
	err = vfs.WriteFile(d.fs, path, data, 0o600)
	if err != nil {
		log.LogError(err, "cannot write content", "path", path)
		d.fs.Remove(path)
		return err
	}
	return nil
}

func (d *Database[O]) DeleteObject(id database.ObjectId) (done bool, err error) {
	if !CheckId(id) {
		return false, fmt.Errorf("invalid id %q", id)
	}
	path := d.OPath(id)
	log := logging.DefaultContext().Logger(REALM)

	d.lock.Lock()
	defer func() {
		if err == nil {
			d.registry.TriggerEvent(id)
		}
	}()
	defer d.lock.Unlock()
	if ok, err := vfs.Exists(d.fs, path); !ok && err == nil {
		return false, database.ErrNotExist
	}
	o, err := d.get(id)
	if err != nil {
		return false, err
	}
	return d._doDeleteObject(log, path, o)
}

func (d *Database[O]) _doDeleteObject(log logging.Logger, path string, o O) (bool, error) {
	if f, ok := generics.TryCast[database.Finalizable](o); ok {
		f.RequestDeletion()
		finalizers := f.GetFinalizers()
		log.Debug("found finalizers for {{path}}: {{finalizers}}", "finalizers", finalizers, "path", path)
		if len(finalizers) != 0 {
			return false, d._doSetObject(log, path, o)
		}
	}
	err := d.fs.Remove(path)
	if err != nil {
		log.LogError(err, "cannot delete file", "path", path)
		return false, err
	}
	log.Debug("deleted object {{path}}", "path", path)
	return true, nil
}

func (d *Database[O]) Path(path string) string {
	return filepath.Join(d.path, path)
}

func (d *Database[O]) OPath(id database.ObjectId) string {
	return filepath.Join(d.path, Path(id))
}
