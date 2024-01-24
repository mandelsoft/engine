package support

import (
	"crypto/sha256"
	"encoding/hex"
	"slices"

	"github.com/mandelsoft/engine/pkg/metamodel/model"
	"github.com/mandelsoft/engine/pkg/utils"
)

func DefaultInputVersion(inputs model.Inputs) string {
	keys := utils.MapKeys(inputs)
	slices.SortFunc(keys, utils.CompareStringable[model.ElementId])

	hash := sha256.New()
	for _, k := range keys {
		v := inputs[k].GetOutputVersion()
		hash.Write([]byte(v))
	}
	h := hash.Sum(nil)
	return hex.EncodeToString(h[:])
}

func UpdateField[T comparable](field *T, value *T, mod ...*bool) bool {
	if value != nil && *field != *value {
		*field = *value
		if len(mod) > 0 {
			*mod[0] = true
		}
		return true
	}
	return false
}

func UpdatePointerField[T comparable](field **T, value *T, mod ...*bool) bool {
	if value != nil && (*field == nil || **field != *value) {
		*field = value
		if len(mod) > 0 {
			*mod[0] = true
		}
		return true
	}
	return false
}
