package runtime

type TypeAccessor interface {
	GetType() string
}

type Object interface {
	TypeAccessor
	SetType(string)
}

type ObjectMeta struct {
	Type string `json:"type"`
}

var _ Object = (*ObjectMeta)(nil)

func (o *ObjectMeta) GetType() string {
	return o.Type
}

func (o *ObjectMeta) SetType(t string) {
	o.Type = t
}
