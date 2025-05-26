package producer

import (
	"context"
	"crypto/tls"
	"log"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/aws_msk_iam_v2"
)

type Producer struct {
	brokers   []string
	mechanism *aws_msk_iam_v2.Mechanism
	transport *kafka.Transport
}

func NewProducer(brokers []string) *Producer {
	cfg, _ := config.LoadDefaultConfig(context.TODO(),
		config.WithRegion("ap-southeast-1"),
		config.WithRetryer(func() aws.Retryer {
			return retry.NewStandard(func(o *retry.StandardOptions) {
				o.MaxAttempts = 10
				o.MaxBackoff = 30 * time.Second
			})
		}),
	)

	// Create SASL mechanism.
	mechanism := aws_msk_iam_v2.NewMechanism(cfg)

	transport := &kafka.Transport{
		SASL: mechanism,
		TLS:  &tls.Config{},
	}

	return &Producer{
		brokers:   brokers,
		mechanism: mechanism,
		transport: transport,
	}
}

func (p *Producer) CreateAndWriteTopic(ctx context.Context, topic string, messages []kafka.Message) error {

	w := kafka.Writer{
		Addr:                   kafka.TCP(p.brokers...),
		Topic:                  topic,
		AllowAutoTopicCreation: true,
		Balancer:               &kafka.Hash{},
		Transport:              p.transport,
		BatchSize:              2000,                 // Number of messages per batch
		BatchTimeout:           1 * time.Millisecond, // Wait time before sending incomplete batch
		RequiredAcks:           kafka.RequireAll,     // Optional: ensure replication
		Async:                  true,                 // Optional: set to true to not block
	}
	defer w.Close()

	retryCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	var writeErr error
	for i := range 5 {
		writeErr = w.WriteMessages(retryCtx, messages...)
		if writeErr == nil {

			log.Printf("Successfully wrote %d messages to topic: %s", len(messages), topic)

			for _, msg := range messages {
				keyString := string(msg.Key)
				msgString := string(msg.Value)
				log.Printf("Message written - Topic: %s, Key: %s, message: %s", topic, keyString, msgString)
			}

			return nil
		}
		log.Printf("Explicit topic creation write attempt %d for topic %s failed: %v", i+1, topic, writeErr)
		time.Sleep(2 * time.Second)
	}
	return writeErr
}
