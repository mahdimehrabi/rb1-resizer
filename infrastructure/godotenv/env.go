package godotenv

import (
	"github.com/joho/godotenv"
	"log"
	"os"
)

type Env struct {
	AMQP          string
	ImageExchange string
	ScrapTopics   string
}

func NewEnv() *Env {
	return &Env{}
}

func (e *Env) Load() {
	if err := godotenv.Load(".env"); err != nil {
		log.Fatal(err)
	}

	e.AMQP = os.Getenv("AMQP")
	e.ImageExchange = os.Getenv("IMAGE_EXCHANGE")
	e.ScrapTopics = os.Getenv("SCRAP_TOPICS")
}
