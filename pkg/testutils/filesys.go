package testutils

import (
	"github.com/mandelsoft/vfs/pkg/composefs"
	"github.com/mandelsoft/vfs/pkg/layerfs"
	"github.com/mandelsoft/vfs/pkg/osfs"
	"github.com/mandelsoft/vfs/pkg/projectionfs"
	"github.com/mandelsoft/vfs/pkg/readonlyfs"
	"github.com/mandelsoft/vfs/pkg/vfs"
)

func TestFileSystem(path string, readonly bool) (vfs.FileSystem, error) {
	tmpfs, err := osfs.NewTempFileSystem()
	if err != nil {
		return nil, err
	}
	defer func() {
		if tmpfs != nil {
			vfs.Cleanup(tmpfs)
		}
	}()

	err = tmpfs.MkdirAll(path, 0700)
	if err != nil {
		return nil, err
	}

	overlay, err := projectionfs.New(osfs.OsFs, path)
	if err != nil {
		return nil, err
	}
	if readonly {
		overlay = readonlyfs.New(overlay)
	} else {
		o, err := projectionfs.New(tmpfs, path)
		if err != nil {
			return nil, err
		}
		overlay = layerfs.New(o, overlay)
	}

	fs := composefs.New(tmpfs, "/tmp")
	err = fs.Mount(path, overlay)
	if err != nil {
		return nil, err
	}

	tmpfs = nil
	return fs, nil
}
