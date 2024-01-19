package database

type Specification[O Object] interface {
	Create(enc Encoding[O]) (Database[O], error)
}
