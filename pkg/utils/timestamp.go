package utils

import (
	"fmt"
	"time"

	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type _time = v1.Time

// Timestamp is time rounded to seconds.
// +k8s:deepcopy-gen=true
type Timestamp struct {
	_time `json:",inline"`
}

func NewTimestamp() Timestamp {
	return Timestamp{
		_time: v1.NewTime(time.Now().UTC().Round(time.Second)),
	}
}

func NewTimestampP() *Timestamp {
	return &Timestamp{
		_time: v1.NewTime(time.Now().UTC().Round(time.Second)),
	}
}

func NewTimestampFor(t time.Time) Timestamp {
	return Timestamp{
		_time: v1.NewTime(t.UTC().Round(time.Second)),
	}
}

func NewTimestampPFor(t time.Time) *Timestamp {
	return &Timestamp{
		_time: v1.NewTime(t.UTC().Round(time.Second)),
	}
}

// MarshalJSON implements the json.Marshaler interface.
// The time is a quoted string in RFC 3339 format, with sub-second precision added if present.
func (t Timestamp) MarshalJSON() ([]byte, error) {
	if y := t.Year(); y < 0 || y >= 10000 {
		// RFC 3339 is clear that years are 4 digits exactly.
		// See golang.org/issue/4556#c15 for more discussion.
		return nil, fmt.Errorf("Time.MarshalJSON: year outside of range [0,9999]")
	}

	b := make([]byte, 0, len(time.RFC3339)+2)
	b = append(b, '"')
	b = t.AppendFormat(b, time.RFC3339)
	b = append(b, '"')
	return b, nil
}

// UnmarshalJSON implements the json.Unmarshaler interface.
// The time is expected to be a quoted string in RFC 3339 format.
func (t *Timestamp) UnmarshalJSON(data []byte) error {
	// Ignore null, like in the main JSON package.
	if string(data) == "null" {
		return nil
	}

	// Fractional seconds are handled implicitly by Parse.
	tt, err := time.Parse(`"`+time.RFC3339+`"`, string(data))
	*t = NewTimestampFor(tt)
	return err
}

func (t Timestamp) String() string {
	return t.Format(time.RFC3339)
}

func (t *Timestamp) Time() time.Time {
	return t._time.Time
}

func (t *Timestamp) Equal(o Timestamp) bool {
	return t._time.Equal(&o._time)
}

func (t *Timestamp) Add(d time.Duration) Timestamp {
	return NewTimestampFor(t._time.Add(d))
}
