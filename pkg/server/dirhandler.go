package server

import (
	"net/http"
	"strings"

	"github.com/mandelsoft/vfs/pkg/osfs"
	"github.com/mandelsoft/vfs/pkg/projectionfs"
	"github.com/mandelsoft/vfs/pkg/vfs"
)

type DirectoryHandler struct {
	fs      vfs.FileSystem
	prefix  string
	handler http.Handler
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
		fs:      fs,
		prefix:  prefix,
		handler: http.StripPrefix(prefix, http.FileServerFS(vfs.AsIoFS(fs))),
	}
}

func (d *DirectoryHandler) RegisterHandler(srv *Server) {
	srv.Handle(d.prefix, d)
}

func (d *DirectoryHandler) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	log.Info("{{method}} serving {{url}}", "method", request.Method, "url", request.URL)
	d.handler.ServeHTTP(writer, request)
}
