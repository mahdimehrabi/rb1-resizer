package service

import (
	"bytes"
	"context"
	"fmt"
	"github.com/google/uuid"
	"github.com/minio/minio-go/v7"
	"rb1-downloader/entity"
	"rb1-downloader/util"
)

type Saver struct {
	minC *minio.Client
}

func NewSaver(minC *minio.Client) *Saver {
	return &Saver{
		minC: minC,
	}
}

func (r *Saver) Download(data []byte) error {
	u, err := entity.FromJSON(data)
	if err != nil {
		return err
	}
	content, err := util.DownloadImage(u.URL)
	if err != nil {
		return err
	}
	reader := bytes.NewReader(content)
	fmt.Println(reader.Len())
	info, err := r.minC.PutObject(context.Background(), "images", uuid.New().String(), reader, int64(reader.Len()), minio.PutObjectOptions{})
	fmt.Println("upload info:", info)
	return err
}
