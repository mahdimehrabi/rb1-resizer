package main

import (
	"github.com/rabbitmq/amqp091-go"
	"log"
	"log/slog"
	"os"
	"rb1-downloader/infrastructure/godotenv"
	"rb1-downloader/service"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdin, nil))
	env := godotenv.NewEnv()
	env.Load()
	ampqConn, err := amqp091.Dial(env.AMQP)
	FatalOnError(err)
	defer ampqConn.Close()
	ch, err := ampqConn.Channel()
	FatalOnError(err)
	err = ch.ExchangeDeclare(env.ImageExchange, "topic", true, false, false, false, nil)
	FatalOnError(err)

	q, err := ch.QueueDeclare(env.QueueName, true, false, false, false, nil)
	FatalOnError(err)
	err = ch.QueueBind(q.Name, "image.", env.ImageExchange, false, nil)
	FatalOnError(err)

	msgs, err := ch.Consume(q.Name, "", false, false, false, false, nil)
	FatalOnError(err)

	saver := service.NewSaver()
	for msg := range msgs {
		if err := saver.Download(msg.Body); err != nil {
			logger.Error("error downloading", err.Error())
		}
		logger.Info("downloaded successfully")
		err = msg.Ack(false)
		FatalOnError(err)
	}
}

func FatalOnError(err error) {
	if err != nil {
		log.Fatal(err.Error())
	}
}
