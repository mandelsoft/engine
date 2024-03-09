package app

type Object map[string]interface{}

func (o Object) SetType(s string) {
	o["type"] = s
}

func (o Object) SetName(s string) {
	o["name"] = s
}

func (o Object) SetNamespace(s string) {
	o["namespace"] = s
}

func (o Object) GetType() string {
	return o["type"].(string)
}

func (o Object) GetNamespace() string {
	return o["namespace"].(string)
}

func (o Object) GetName() string {
	return o["name"].(string)
}

func (o Object) GetStatus() string {
	s := o["status"]
	if s == nil {
		return ""
	}
	if str, ok := s.(string); ok {
		return str
	}
	if m, ok := s.(map[string]interface{}); ok {
		s := m["status"]
		if s != nil {
			if str, ok := s.(string); ok {
				return str
			}
		}
	}
	return ""
}

type List struct {
	Items []Object `json:"items"`
}
