package database

type Specification interface {
	Create(enc Encoding) (Database, error)
}
