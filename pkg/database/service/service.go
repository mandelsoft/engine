package service

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/mandelsoft/engine/pkg/database"
	"github.com/mandelsoft/engine/pkg/runtime"
	"github.com/mandelsoft/engine/pkg/server"
)

type DatabaseAccess[O database.Object] struct {
	database database.Database[O]
	prefix   string
}

func New[O database.Object](db database.Database[O], prefix string) *DatabaseAccess[O] {
	if !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}
	return &DatabaseAccess[O]{
		database: db,
		prefix:   prefix,
	}
}

func (a *DatabaseAccess[O]) RegisterHandler(src *server.Server) {
	src.Handle(a.prefix, a)
}

func (a *DatabaseAccess[O]) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	var data []byte
	status := http.StatusOK

	path := req.URL.Path[len(a.prefix):]
	fmt.Printf("%s: path %s\n", req.Method, path)
	comps := strings.Split(path, "/")

	typ := comps[0]
	if path == "" {
		e := &Error{"invalid path"}
		data, _ = json.Marshal(e)
		status = http.StatusInternalServerError
	} else {
		switch req.Method {
		case http.MethodGet:
			if len(comps) < 2 {
				e := &Error{"invalid path"}
				data, _ = json.Marshal(e)
				status = http.StatusInternalServerError
			} else {
				name := comps[len(comps)-1]
				ns := strings.Join(comps[1:len(comps)-1], "/")
				oid := database.NewObjectId(typ, ns, name)

				obj, err := a.database.GetObject(oid)
				if err != nil {
					if errors.Is(err, database.ErrNotExist) {
						status = http.StatusNotFound
					} else {
						e := &Error{err.Error()}
						data, _ = json.Marshal(e)
						status = http.StatusInternalServerError
					}
				} else {
					data, _ = json.Marshal(obj)
				}
			}

		case http.MethodDelete:
			if len(comps) < 2 {
				e := &Error{"invalid path"}
				data, _ = json.Marshal(e)
				status = http.StatusInternalServerError
			}
			name := comps[len(comps)-1]
			ns := strings.Join(comps[1:len(comps)-1], "/")
			oid := database.NewObjectId(typ, ns, name)

			deleted, err := a.database.DeleteObject(oid)
			if err != nil {
				if errors.Is(err, database.ErrNotExist) {
					status = http.StatusNotFound
				} else {
					e := &Error{err.Error()}
					data, _ = json.Marshal(e)
					status = http.StatusInternalServerError
				}
			} else {
				if !deleted {
					status = http.StatusAccepted
				}
			}

		case http.MethodPost:
			if len(comps) < 2 {
				e := &Error{"invalid path"}
				data, _ = json.Marshal(e)
				status = http.StatusInternalServerError
			}
			name := comps[len(comps)-1]
			ns := strings.Join(comps[1:len(comps)-1], "/")
			oid := database.NewObjectId(typ, ns, name)

			data, err := io.ReadAll(req.Body)
			if err != nil {
				e := &Error{err.Error()}
				data, _ = json.Marshal(e)
				status = http.StatusInternalServerError
			} else {
				t := req.Header.Get("Content-Type")
				if t != "" && t != "application/json" {
					status = http.StatusUnsupportedMediaType
				} else {
					obj, err := a.database.SchemeTypes().(runtime.Encoding[O]).Decode(data)
					if err != nil {
						e := &Error{err.Error()}
						data, _ = json.Marshal(e)
						status = http.StatusBadRequest
					} else {
						msg := ""
						if obj.GetName() != name {
							msg = "name mismatch"
						}
						if obj.GetNamespace() != ns {
							msg = "namespace mismatch"
						}
						if obj.GetType() != typ {
							msg = "type mismatch"
						}
						if msg != "" {
							status = http.StatusBadRequest
						} else {
							_, err := a.database.GetObject(oid)
							if err == database.ErrNotExist {
								// TODO: non-atomic operation
								status = http.StatusCreated
							}
							err = a.database.SetObject(obj)
							if err != nil {
								msg = err.Error()
								status = http.StatusConflict
							}
						}
						if msg != "" {
							e := &Error{msg}
							data, _ = json.Marshal(e)
						}
					}
				}
			}
		case "LIST":
			closure := false
			ns := ""
			if len(comps) > 1 {
				ns = strings.Join(comps[1:], "/")
			}
			if strings.HasSuffix(ns, "*") {
				ns = ns[:len(ns)-1]
				closure = true
			}
			if typ == "*" {
				typ = ""
			}
			fmt.Printf("ns=%s(%t), typ=%s\n", ns, closure, typ)
			list, err := a.database.ListObjects(typ, closure, ns)
			if err == nil {
				data, err = json.Marshal(&Items[O]{Items: list})
			}
			if err != nil {
				e := &Error{err.Error()}
				data, _ = json.Marshal(e)
				status = http.StatusInternalServerError
			}
		default:
			status = http.StatusMethodNotAllowed
		}
	}

	w.WriteHeader(status)
	if data != nil {
		w.Write(data)
	}
}

type Error struct {
	Error string `json:"error"`
}

type Items[O database.Object] struct {
	Items []O `json:"items"`
}
