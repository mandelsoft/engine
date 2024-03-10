package server

import (
	"io"
	"net/http"
	"strings"

	"github.com/mandelsoft/vfs/pkg/osfs"
	"github.com/mandelsoft/vfs/pkg/projectionfs"
	"github.com/mandelsoft/vfs/pkg/vfs"
)

type DirectoryHandler struct {
	fs     vfs.FileSystem
	prefix string
}

var _ http.Handler = (*DirectoryHandler)(nil)

func NewDirectoryHandlerFor(path, prefix string) (*DirectoryHandler, error) {
	fs, err := projectionfs.New(osfs.OsFs, path)
	if err != nil {
		return nil, err
	}
	return NewDirectoryHandler(fs, prefix), nil
}

func NewDirectoryHandler(fs vfs.FileSystem, prefix string) *DirectoryHandler {
	if !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}
	return &DirectoryHandler{
		fs:     fs,
		prefix: prefix,
	}
}

func (d *DirectoryHandler) RegisterHandler(srv *Server) {
	srv.Handle(d.prefix, d)
}

func (d *DirectoryHandler) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		writer.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	path := request.URL.Path[len(d.prefix):]

	file, err := d.fs.Open(path)
	if err != nil {
		writer.WriteHeader(http.StatusNotFound)
	} else {
		io.Copy(writer, file)
		file.Close()
	}
}
