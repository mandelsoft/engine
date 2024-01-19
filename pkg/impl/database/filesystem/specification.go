package filesystem

import (
	"github.com/mandelsoft/engine/pkg/database"
	"github.com/mandelsoft/engine/pkg/utils"
	"github.com/mandelsoft/vfs/pkg/osfs"
	"github.com/mandelsoft/vfs/pkg/vfs"
)

type Specification[O database.Object] struct {
	Path       string
	FileSystem vfs.FileSystem
}

var _ database.Specification[database.Object] = (*Specification[database.Object])(nil)

func NewSpecification[O database.Object](path string, fss ...vfs.FileSystem) *Specification[O] {
	return &Specification[O]{
		Path:       path,
		FileSystem: utils.OptionalDefaulted(osfs.New(), fss...),
	}
}

func (s *Specification[O]) Create(enc database.Encoding[O]) (database.Database[O], error) {
	return New[O](enc, s.Path, s.FileSystem)
}
