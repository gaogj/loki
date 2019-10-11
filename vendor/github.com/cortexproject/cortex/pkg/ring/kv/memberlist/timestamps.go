package memberlist

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"time"
)

// This is a simple state that is gossiped under specific key by memberlist_client itself.
// Each node updates its own timestamp.
type timestamps struct {
	// Before changing this structure, read https://golang.org/pkg/encoding/gob/ carefully first.
	Timestamps map[string]time.Time
}

func (t *timestamps) Merge(other Mergeable) (change Mergeable, err error) {
	tsp, ok := other.(*timestamps)
	if !ok {
		return nil, fmt.Errorf("expected timestamps, got %T", other)
	}

	if t.Timestamps == nil {
		t.Timestamps = make(map[string]time.Time)
	}

	upd := make(map[string]time.Time)

	if tsp.Timestamps != nil {
		for n, nt := range tsp.Timestamps {
			if nt.After(t.Timestamps[n]) {
				t.Timestamps[n] = nt
				upd[n] = nt
			}
		}
	}

	return &timestamps{upd}, nil
}

func (t *timestamps) MergeContent() []string {
	c := []string(nil)
	for n, _ := range t.Timestamps {
		c = append(c, n)
	}
	return c
}

func (t *timestamps) RemoveTombstones(limit time.Time) {
	for n, nt := range t.Timestamps {
		if nt.Before(limit) {
			delete(t.Timestamps, n)
		}
	}
}

func newTimestamps() *timestamps {
	return &timestamps{Timestamps: make(map[string]time.Time)}
}

type timestampsCodec struct {
}

func (t timestampsCodec) Encode(val interface{}) ([]byte, error) {
	if ts, ok := val.(*timestamps); ok && ts != nil {
		buf := bytes.Buffer{}
		err := gob.NewEncoder(&buf).Encode(ts)
		return buf.Bytes(), err
	}
	return nil, fmt.Errorf("invalid type: %T, value: %v", val, val)
}

func (t timestampsCodec) Decode(buf []byte) (interface{}, error) {
	ts := newTimestamps()
	err := gob.NewDecoder(bytes.NewBuffer(buf)).Decode(ts)
	return ts, err
}
