package objectbase

type Specification interface {
	SchemeTypes() SchemeTypes
	CreateObjectbase() (Objectbase, error)
}
