package entity

import (
	"encoding/json"
)

type URL struct {
	URL   string `json:"name"`
	Query string `json:"query"`
}

func (u *URL) JSON() ([]byte, error) {
	return json.Marshal(u)
}

func (u *URL) UnmarshalJSON(data []byte) error {
	return json.Unmarshal(data, u)
}
