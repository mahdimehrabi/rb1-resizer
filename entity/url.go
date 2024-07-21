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

func FromJSON(data []byte) (*URL, error) {
	var u URL
	if err := json.Unmarshal(data, &u); err != nil {
		return nil, err
	}
	return &u, nil
}
