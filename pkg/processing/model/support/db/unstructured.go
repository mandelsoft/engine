package db

import (
	"encoding/json"
	"fmt"

	"sigs.k8s.io/yaml"
)

type Unstructured struct {
	ObjectMeta
	Other map[string]interface{}
}

var _ Object = (*Unstructured)(nil)
var _ json.Unmarshaler = (*Unstructured)(nil)
var _ json.Marshaler = Unstructured{}

func (u *Unstructured) GetStatus() interface{} {
	return u.Other["status"]
}

func (u *Unstructured) SetStatus(s interface{}) {
	if s == nil {
		delete(u.Other, "status")
	} else {
		u.Other["status"] = s
	}
}

func (u *Unstructured) GetStatusValue() string {
	status := u.Other["status"]
	if status == nil {
		return ""
	}
	if m, ok := status.(map[string]interface{}); ok {
		s := m["status"]
		if s == nil {
			return ""
		}
		if st, ok := s.(string); ok {
			return st
		}
	}
	return ""
}

func (u *Unstructured) UnmarshalJSON(data []byte) error {
	err := yaml.Unmarshal(data, &u.ObjectMeta)
	if err != nil {
		return err
	}

	u.Other = map[string]interface{}{}
	err = yaml.Unmarshal(data, &u.Other)
	if err != nil {
		return err
	}
	delete(u.Other, "kind")
	delete(u.Other, "apiVersion")
	delete(u.Other, "metadata")
	return nil
}

func (u Unstructured) MarshalJSON() ([]byte, error) {
	data, err := json.Marshal(u.Other)
	if err != nil {
		return nil, err
	}

	var r map[string]interface{}
	err = json.Unmarshal(data, &r)
	if err != nil {
		return nil, err
	}
	err = u.ObjectMeta.addTo(r)
	if err != nil {
		return nil, err
	}
	return json.Marshal(r)
}

func UnstructuredFor(in any) (*Unstructured, error) {
	switch d := in.(type) {
	case *Unstructured:
		return d, nil
	case map[string]interface{}:
		data, err := json.Marshal(d)
		if err != nil {
			return nil, err
		}
		in = data
	case Object:
		data, err := json.Marshal(d)
		if err != nil {
			return nil, err
		}
		in = data
	}

	if data, ok := in.([]byte); ok {
		var u Unstructured

		err := yaml.Unmarshal(data, &u)
		if err != nil {
			return nil, err
		}
		return &u, nil
	}
	return nil, fmt.Errorf("unknown object format %T", in)
}
