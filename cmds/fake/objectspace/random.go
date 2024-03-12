package objectspace

import (
	"math/rand"
)

func Random[E any](list []E) E {
	return list[rand.Intn(len(list))]
}
