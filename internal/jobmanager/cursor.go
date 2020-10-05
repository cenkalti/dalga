package jobmanager

import (
	"encoding/base64"
	"encoding/json"
)

type Cursor struct {
	Path      string
	SortBy    string
	Reverse   bool
	LastValue string
	Limit     int64
}

func (c Cursor) Encode() string {
	b, _ := json.Marshal(&c)
	return base64.StdEncoding.EncodeToString(b)
}

func (c *Cursor) Decode(s string) error {
	b, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		return err
	}
	return json.Unmarshal(b, c)
}
