package consumer

import (
	"context"
	"log"
	"time"

	"crypto/tls"

	"github.com/segmentio/kafka-go"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/segmentio/kafka-go/sasl/aws_msk_iam_v2"
)

// Consumer wraps a Kafka reader.
type Consumer struct {
	Reader *kafka.Reader
}

// NewConsumer creates a new Kafka consumer.
func NewConsumer(brokers []string, topic string) *Consumer {
	cfg, err := config.LoadDefaultConfig(context.TODO(), config.WithRegion("ap-southeast-1"))
	if err != nil {
		panic(err)
	}
	mechanism := aws_msk_iam_v2.NewMechanism(cfg)

	return &Consumer{
		Reader: kafka.NewReader(kafka.ReaderConfig{
			Brokers:     brokers,
			Topic:       topic,
			GroupID:     "my-app-consumer-9",
			MaxBytes:    10e6, // 10MB
			StartOffset: kafka.FirstOffset,
			Dialer: &kafka.Dialer{
				Timeout:       10 * time.Second,
				DualStack:     true,
				SASLMechanism: mechanism,
				TLS:           &tls.Config{},
			},
		}),
	}
}

// Consume continuously reads messages and calls the provided handler.
func (c *Consumer) Consume(ctx context.Context, handler func(message kafka.Message)) error {
	for {
		msg, err := c.Reader.ReadMessage(ctx)
		if err != nil {
			// Handle context cancellation properly
			if ctx.Err() != nil {
				log.Println("Consumer context canceled, stopping consumption")
				return ctx.Err()
			}

			log.Printf("Error reading message: %v", err)
			return err
		}
		handler(msg)
		if err := c.Reader.CommitMessages(ctx, msg); err != nil {
			log.Printf("Failed to commit message: %v", err)
		}
	}
}

// Close closes the underlying reader.
func (c *Consumer) Close() error {
	return c.Reader.Close()
}
