// Package kafka wraps segmentio/kafka-go for this pipeline.
package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	kafkago "github.com/segmentio/kafka-go"

	"html-kafka-pipeline/internal/models"
)

// Producer publishes extracted table rows to a Kafka topic.
type Producer struct {
	writer *kafkago.Writer
}

// NewProducer creates a synchronous Kafka producer.
func NewProducer(brokers []string, topic string) *Producer {
	w := &kafkago.Writer{
		Addr:         kafkago.TCP(brokers...),
		Topic:        topic,
		Balancer:     &kafkago.RoundRobin{},
		RequiredAcks: kafkago.RequireOne,
		BatchSize:    200,
		BatchTimeout: 100 * time.Millisecond,
		Async:        false,
		// Retry delivery on transient broker errors.
		MaxAttempts: 5,
	}
	return &Producer{writer: w}
}

// Publish sends a batch of messages to Kafka in a single write call.
func (p *Producer) Publish(ctx context.Context, msgs []models.Message) error {
	if len(msgs) == 0 {
		return nil
	}

	kmsgs := make([]kafkago.Message, len(msgs))
	for i, m := range msgs {
		b, err := json.Marshal(m)
		if err != nil {
			return fmt.Errorf("marshal message %d: %w", i, err)
		}
		kmsgs[i] = kafkago.Message{Value: b}
	}

	if err := p.writer.WriteMessages(ctx, kmsgs...); err != nil {
		return fmt.Errorf("write messages to kafka: %w", err)
	}

	slog.Info("published to kafka", "count", len(msgs))
	return nil
}

// Close flushes and closes the underlying writer.
func (p *Producer) Close() error {
	return p.writer.Close()
}
