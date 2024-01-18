package processing

type State interface {
	GetLinks() []Element
	GetVersion() string
}
