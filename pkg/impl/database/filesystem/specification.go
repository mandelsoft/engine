package filesystem

import (
	"github.com/mandelsoft/engine/pkg/database"
	"github.com/mandelsoft/engine/pkg/utils"
	"github.com/mandelsoft/vfs/pkg/osfs"
	"github.com/mandelsoft/vfs/pkg/vfs"
)

type Specification struct {
	Path       string
	FileSystem vfs.FileSystem
}

var _ database.Specification = (*Specification)(nil)

func NewSpecification(path string, fss ...vfs.FileSystem) *Specification {
	return &Specification{
		Path:       path,
		FileSystem: utils.OptionalDefaulted(osfs.New(), fss...),
	}
}

func (s *Specification) Create(enc database.Encoding) (database.Database, error) {
	return New(enc, s.Path, s.FileSystem)
}
