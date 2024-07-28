package entity

import "encoding/json"

type Image struct {
	Name       string
	BucketName string
}

func (i *Image) Marshal() ([]byte, error) {
	return json.Marshal(i)
}
