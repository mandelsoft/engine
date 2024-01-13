package filesystem

import (
	"fmt"

	"github.com/mandelsoft/engine/pkg/database"
)

func Path(o database.ObjectId) string {
	return fmt.Sprintf("%s/%s/%s.yaml", o.GetType(), o.GetNamespace(), o.GetName())
}
