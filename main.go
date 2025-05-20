package main

import (
	"context"
	"log"
	"os"
	"strings"

	"kafka-go/messageHandlers"
	"kafka-go/service/consumer"
	"kafka-go/service/producer"
	"kafka-go/utils"

	"github.com/joho/godotenv"
	"github.com/segmentio/kafka-go"
)

func init() {
	if err := utils.LoadFieldMappings(); err != nil {
		log.Fatal("Error loading field mappings in field_mappings:", err)
	}
}

func main() {
	log.Printf("SMT Start running")
	// Load .env
	if err := godotenv.Load(); err != nil {
		log.Fatal("Error loading .env file")
	}

	brokers := strings.Split(os.Getenv("BROKERS"), ",")
	inputTopics := strings.Split(os.Getenv("INPUT_TOPICS"), ",")

	if len(inputTopics) == 0 || inputTopics[0] == "" {
		log.Fatal("INPUT_TOPICS environment variable is not set or is empty")
	}

	log.Printf("Watching %d topic(s):", len(inputTopics))
	for i, topic := range inputTopics {
		trimmedTopic := strings.TrimSpace(topic)
		if trimmedTopic != "" {
			log.Printf("  [%d] %s", i+1, trimmedTopic)
		}
	}

	ctx := context.Background()
	prod := producer.NewProducer(brokers)

	for _, inputTopic := range inputTopics {
		cons := consumer.NewConsumer(brokers, inputTopic)

		go func(c *consumer.Consumer, outTopic string) {
			defer c.Close()
			if err := c.Consume(ctx, func(msg kafka.Message) {
				if msg.Value == nil {
					log.Printf("null message")
					return
				} else {
					messageHandlers.ProcessMessage(msg.Key, msg.Value, prod, ctx, outTopic)
				}
			}); err != nil {
				log.Printf("Consumer error: %v", err)
			}
		}(cons, inputTopic)
	}

	select {}
}
