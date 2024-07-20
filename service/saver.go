package service

import (
	"rb1-downloader/entity"
)

type Saver struct {
}

func NewSaver() *Saver {
	return &Saver{}
}

func (r *Saver) Download(data []byte) error {
	u := entity.URL{}
	if err := u.UnmarshalJSON(data); err != nil {
		return err
	}

	return nil
}
