package database

type Specification[O Object] interface {
	Create(enc SchemeTypes[O]) (Database[O], error)
}
