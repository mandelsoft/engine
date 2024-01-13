package pool

import (
	"fmt"
	"strings"

	"github.com/mandelsoft/engine/pkg/database"
)

func DecodeKey(key string) (Command, database.ObjectId, error) {
	i := strings.Index(key, ":")

	if i < 0 {
		return Command(key), nil, nil
	}

	main := key[:i]
	if main == "cmd" {
		return Command(key[i+1:]), nil, nil
	}
	if main == "obj" {
		key = key[i+1:]
	}

	typ, namespace, name, err := DecodeObjectKey(key)
	if err != nil {
		return "", nil, fmt.Errorf("error decoding '%s': %s", key, err)
	}
	id := database.NewObjectId(typ, namespace, name)
	return "", id, err
}

func EncodeCommandKey(cmd Command) string {
	return fmt.Sprintf("cmd:%s", cmd)
}

func EncodeObjectKeyForObject(o database.ObjectId) string {
	return EncodeObjectKey(o.GetType(), o.GetNamespace(), o.GetName())
}

func EncodeObjectKey(typ, ns, name string) string {
	return fmt.Sprintf("obj:%s:%s:%s", typ, ns, name)
}

func DecodeObjectKey(key string) (typ, namespace, name string, err error) {
	parts := strings.Split(key, ":")
	if len(parts) != 3 {
		return "", "", "", fmt.Errorf("unexpected key format: %q", key)
	}
	// kind, namespace and name
	return parts[0], parts[1], parts[2], nil
}
