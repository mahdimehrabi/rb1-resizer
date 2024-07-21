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
	minC      *minio.Client
	queue     <-chan amqp091.Delivery
	logger    *slog.Logger
	ch        *amqp091.Channel
	queueName string
}

func NewSaver(minC *minio.Client, logger *slog.Logger, ch *amqp091.Channel, queueName string) *Saver {
	s := &Saver{
		minC:      minC,
		logger:    logger,
		ch:        ch,
		queueName: queueName,
	}

	for range 50 {
		go s.Worker()
	}
	return s
}

func (s *Saver) Setup() error {
	deliveries, err := s.ch.Consume(s.queueName, "", false, false, false, false, nil)
	s.queue = deliveries
	return err
}

func (s *Saver) Worker() {
	for msg := range s.queue {
		reader := bytes.NewReader(msg.Body)
		id := uuid.New().String()
		_, err := s.minC.PutObject(context.Background(), "images", id, reader, int64(reader.Len()), minio.PutObjectOptions{})
		if err != nil {
			if err := msg.Nack(false, false); err != nil {
				s.logger.Error("failed to nack", err.Error())
			}
			s.logger.Error("failed to save", err.Error())
			return
		}
		if err := msg.Ack(false); err != nil {
			s.logger.Error("failed to ack", err.Error())
			if err := s.minC.RemoveObject(context.Background(), "images", id, minio.RemoveObjectOptions{}); err != nil {
				s.logger.Error("failed to remove object", err.Error())
				return
			}
		}
		fmt.Println("saved successfully", time.Now().UnixNano())
	}
}
