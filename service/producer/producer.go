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

// func (p *Producer) ProduceBatch(ctx context.Context, topic string, messages []kafka.Message) error {
// 	dialer := &kafka.Dialer{
// 		Timeout:       90 * time.Second,
// 		DualStack:     true,
// 		SASLMechanism: p.mechanism,
// 		TLS:           &tls.Config{},
// 	}

// 	writer := kafka.NewWriter(kafka.WriterConfig{
// 		Brokers:  p.brokers,
// 		Topic:    topic,
// 		Balancer: &kafka.LeastBytes{},
// 		Dialer:   dialer,
// 	})
// 	defer writer.Close()

// 	attemptTimeout := 90 * time.Second
// 	maxRetries := 10

// 	var writeErr error
// 	for i := range maxRetries {

// 		retryCtx, cancel := context.WithTimeout(ctx, attemptTimeout)
// 		defer cancel()

// 		writeErr = writer.WriteMessages(retryCtx, messages...)
// 		if writeErr == nil {
// 			return nil
// 		}
// 		log.Printf("Write attempt %d for topic %s failed: %v", i+1, topic, writeErr)
// 		if strings.Contains(writeErr.Error(), "Unknown Topic Or Partition") {
// 			log.Printf("Topic %s not found, attempting explicit creation", topic)
// 			if err := p.createAndWriteTopic(ctx, topic, messages); err != nil {
// 				log.Printf("Failed to create topic %s: %v", topic, err)
// 			} else {
// 				return nil
// 			}
// 		}
// 		time.Sleep(2 * time.Second)
// 	}
// 	return writeErr
// }

func (p *Producer) CreateAndWriteTopic(ctx context.Context, topic string, messages []kafka.Message) error {

	w := kafka.Writer{
		Addr:                   kafka.TCP(p.brokers...),
		Topic:                  topic,
		AllowAutoTopicCreation: true,
		Balancer:               &kafka.Hash{},
		Transport:              p.transport,
		BatchSize:              300,                   // Number of messages per batch
		BatchTimeout:           10 * time.Millisecond, // Wait time before sending incomplete batch
		RequiredAcks:           kafka.RequireAll,      // Optional: ensure replication
		Async:                  false,                 // Optional: set to true to not block
	}
	defer w.Close()

	retryCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	var writeErr error
	for i := range 5 {
		writeErr = w.WriteMessages(retryCtx, messages...)
		if writeErr == nil {
			return nil
		}
		log.Printf("Explicit topic creation write attempt %d for topic %s failed: %v", i+1, topic, writeErr)
		time.Sleep(2 * time.Second)
	}
	return writeErr
}
