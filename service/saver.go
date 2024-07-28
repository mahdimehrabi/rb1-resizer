package service

import (
	"bytes"
	"context"
	"fmt"
	"github.com/google/uuid"
	"github.com/minio/minio-go/v7"
	"github.com/rabbitmq/amqp091-go"
	"log/slog"
	"rb1-downloader/entity"
	"rb1-downloader/util"
	"time"
)

type Saver struct {
	minC      *minio.Client
	queue     <-chan amqp091.Delivery
	logger    *slog.Logger
	ch        *amqp091.Channel
	queueName string
	exchange  string
}

func NewSaver(minC *minio.Client, logger *slog.Logger, ch *amqp091.Channel, queueName string, exchange string) *Saver {
	s := &Saver{
		minC:      minC,
		logger:    logger,
		ch:        ch,
		queueName: queueName,
		exchange:  exchange,
	}
	return s
}

func (s *Saver) Setup() error {
	deliveries, err := s.ch.Consume(s.queueName, "", false, false, false, false, nil)
	s.queue = deliveries
	for range 50 {
		go s.Worker()
	}
	return err
}

func (s *Saver) Worker() {
	for msg := range s.queue {
		url, err := entity.FromJSON(msg.Body)
		if err != nil {
			msg.Nack(false, false)
			s.logger.Error(fmt.Sprintf("Invalid json:%s", err.Error()))
			return
		}

		content, err := util.DownloadImage(url.URL)
		if err != nil {
			if err := msg.Nack(false, false); err != nil {
				s.logger.Error("failed to nack", err.Error())
			}
			s.logger.Error("Error failed to download:%s", err.Error())
			return
		}

		reader := bytes.NewReader(content)
		id := uuid.New().String()
		_, err = s.minC.PutObject(context.Background(), "image-resize", id, reader, int64(reader.Len()), minio.PutObjectOptions{})
		if err != nil {
			if err := msg.Nack(false, false); err != nil {
				s.logger.Error("failed to nack", err.Error())
			}
			s.logger.Error("failed to save", err.Error())
			return
		}

		if err := msg.Ack(false); err != nil {
			s.logger.Error("failed to ack", err.Error())
			if err := s.minC.RemoveObject(context.Background(), "images.#", id, minio.RemoveObjectOptions{}); err != nil {
				s.logger.Error("failed to remove object", err.Error())
			}
			return
		}
		img := entity.Image{
			id,
			"images",
		}
		image, err := img.Marshal()
		err = s.ch.Publish(s.exchange, "downloaded."+url.Query, false, false, amqp091.Publishing{
			Body:        image,
			ContentType: "application/json",
		})

		if err != nil {
			if err := s.minC.RemoveObject(context.Background(), "images", id, minio.RemoveObjectOptions{}); err != nil {
				s.logger.Error("failed to remove object", err.Error())
			}
			if err := msg.Nack(false, false); err != nil {
				s.logger.Error("failed to nack", err.Error())
			}
			s.logger.Error("failed to publish", err.Error())
			return
		}
		s.logger.Info("saved successfully", time.Now().UnixNano())
	}
}
