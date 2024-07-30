package godotenv

import (
	"fmt"
	"github.com/joho/godotenv"
	"os"
	"strings"
)

type Env struct {
	AMQP             string
	ImageExchange    string
	ScrapTopics      []string
	QueueName        string
	MinioHost        string
	MinioAccessToken string
	MinioSecret      string
}

func NewEnv() *Env {
	return &Env{}
}

func (e *Env) Load() {
	if err := godotenv.Load(".env"); err != nil {
		fmt.Println(err.Error())
	}

	e.AMQP = os.Getenv("AMQP")
	e.ImageExchange = os.Getenv("IMAGE_EXCHANGE")
	scrapTopics := os.Getenv("SCRAP_TOPICS")
	for _, topic := range strings.Split(scrapTopics, ",") {
		e.ScrapTopics = append(e.ScrapTopics, topic)
	}
	e.QueueName = os.Getenv("QUEUE_NAME")
	e.MinioHost = os.Getenv("MINIO_HOST")
	e.MinioAccessToken = os.Getenv("MINIO_ACCESS_TOKEN")
	e.MinioSecret = os.Getenv("MINIO_SECRET")
}
