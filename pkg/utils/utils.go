package utils

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"reflect"
	"slices"

	"github.com/gowebpki/jcs"
	"github.com/modern-go/reflect2"
)

func Optional[T any](args ...T) T {
	var _nil T
	for _, e := range args {
		if !reflect.DeepEqual(e, _nil) {
			return e
		}
	}
	return _nil
}

func OptionalDefaulted[T any](def T, args ...T) T {
	var _nil T
	for _, e := range args {
		if !reflect.DeepEqual(e, _nil) {
			return e
		}
	}
	return def
}

func HashData(d interface{}) string {
	if reflect2.IsNil(d) {
		return ""
	}
	var err error
	var data []byte
	switch b := d.(type) {
	case []byte:
		data = b
	case string:
		data = []byte(b)
	default:
		data, err = json.Marshal(d)
		if err != nil {
			panic(err)
		}
		data, err = jcs.Transform(data)
		if err != nil {
			panic(err)
		}
	}
	h := sha256.Sum256(data)
	return hex.EncodeToString(h[:])
}

func Cycle[T comparable](id T, stack ...T) []T {
	i := slices.Index(stack, id)
	if i < 0 {
		return nil
	}
	return append(slices.Clone(stack[i:]), id)
}
