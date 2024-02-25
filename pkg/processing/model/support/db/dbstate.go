package db

type StandardCurrentState struct {
	ObservedVersion string `json:"observedVersion,omitempty"`
	FormalVersion   string `json:"formalVersion,omitempty"`

	InputVersion  string `json:"inputVersion,omitempty"`
	ObjectVersion string `json:"objectVersion,omitempty"`
	OutputVersion string `json:"outputVersion,omitempty"`
}

var _ CurrentState = (*StandardCurrentState)(nil)

func (d *StandardCurrentState) GetFormalVersion() string {
	return d.FormalVersion
}

func (d *StandardCurrentState) SetFormalVersion(v string) bool {
	if d.FormalVersion == v {
		return false
	}
	d.FormalVersion = v
	return true
}

func (d *StandardCurrentState) GetObservedVersion() string {
	return d.ObservedVersion
}

func (d *StandardCurrentState) SetObservedVersion(v string) bool {
	if d.ObservedVersion == v {
		return false
	}
	d.ObservedVersion = v
	return true
}

func (d *StandardCurrentState) GetObjectVersion() string {
	return d.ObjectVersion
}

func (d *StandardCurrentState) SetObjectVersion(v string) bool {
	if d.ObjectVersion == v {
		return false
	}
	d.ObjectVersion = v
	return true
}

func (d *StandardCurrentState) GetInputVersion() string {
	return d.InputVersion
}

func (d *StandardCurrentState) SetInputVersion(v string) bool {
	if d.InputVersion == v {
		return false
	}
	d.InputVersion = v
	return true
}

func (d *StandardCurrentState) GetOutputVersion() string {
	return d.OutputVersion
}

func (d *StandardCurrentState) SetOutputVersion(v string) bool {
	if d.OutputVersion == v {
		return false
	}
	d.OutputVersion = v
	return true
}

////////////////////////////////////////////////////////////////////////////////

type StandardTargetState struct {
	ObjectVersion       string `json:"objectVersion,omitempty"`
	FormalObjectVersion string `json:"formalObjectVersion,omitempty"`
}

var _ TargetState = (*StandardTargetState)(nil)

func (d *StandardTargetState) GetFormalObjectVersion() string {
	return d.FormalObjectVersion
}

func (d *StandardTargetState) SetFormalObjectVersion(v string) bool {
	if d.FormalObjectVersion == v {
		return false
	}
	d.FormalObjectVersion = v
	return true
}

func (d *StandardTargetState) GetObjectVersion() string {
	return d.ObjectVersion
}

func (d *StandardTargetState) SetObjectVersion(v string) bool {
	if d.ObjectVersion == v {
		return false
	}
	d.ObjectVersion = v
	return true
}
