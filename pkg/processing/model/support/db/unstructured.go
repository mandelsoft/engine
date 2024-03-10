package db

import (
	"encoding/json"
	"fmt"

	"sigs.k8s.io/yaml"
)

type Unstructured struct {
	ObjectMeta `json:",inline"`
	Spec       json.RawMessage `json:"spec,omitempty"`
	Status     json.RawMessage `json:"status,omitempty"`
}

var _ Object = (*Unstructured)(nil)

func (u *Unstructured) GetStatusValue() string {
	var m map[string]interface{}

	err := json.Unmarshal(u.Status, &m)
	if err != nil {
		return ""
	}
	s := m["status"]
	if s == nil {
		return ""
	}
	if st, ok := s.(string); ok {
		return st
	}
	return ""
}

func (u *Unstructured) SetStatus(s json.RawMessage) {
	u.Status = s
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
