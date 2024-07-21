package service

import (
	"bytes"
	"context"
	"fmt"
	"github.com/google/uuid"
	"github.com/minio/minio-go/v7"
	"github.com/rabbitmq/amqp091-go"
	"log/slog"
	"time"
)

type Saver struct {
	minC   *minio.Client
	queue  <-chan amqp091.Delivery
	logger *slog.Logger
}

func NewSaver(minC *minio.Client, logger *slog.Logger) *Saver {
	s := &Saver{
		minC:   minC,
		logger: logger,
	}
	for range 50 {
		go s.Worker()
	}
	return s
}

func (s *Saver) Worker() {
	for msg := range s.queue {
		reader := bytes.NewReader(msg.Body)
		_, err := s.minC.PutObject(context.Background(), "images", uuid.New().String(), reader, int64(reader.Len()), minio.PutObjectOptions{})
		if err != nil {
			if err := msg.Nack(false, false); err != nil {
				s.logger.Error("failed to nack", err.Error())
			}
			s.logger.Error("failed to save", err.Error())
		}
		if err := msg.Ack(false); err != nil {
			s.logger.Error("failed to ack", err.Error())
		}
		fmt.Println("saved successfully", time.Now().UnixNano())
	}
}

func (s *Saver) SetDelivery(delivery <-chan amqp091.Delivery) {
	s.queue = delivery
}
