package main

import (
	"github.com/rabbitmq/amqp091-go"
	"log"
	"log/slog"
	"os"
	"rb1-downloader/entity"
	"rb1-downloader/infrastructure/godotenv"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdin, nil))
	iuc := make(chan *entity.URL, 10)

	env := godotenv.NewEnv()
	env.Load()
	ampqConn, err := amqp091.Dial(env.AMQP)
	FatalOnError(err)
	defer ampqConn.Close()
	ch, err := ampqConn.Channel()
	FatalOnError(err)
	err = ch.ExchangeDeclare(env.ImageExchange, "topic", true, false, false, false, nil)
	FatalOnError(err)

	for i := range iuc {
		if err != nil {
			logger.Error("error producing", err.Error())
		}
		logger.Info("sent", i.URL)
	}
}

func FatalOnError(err error) {
	if err != nil {
		log.Fatal(err.Error())
	}
}
