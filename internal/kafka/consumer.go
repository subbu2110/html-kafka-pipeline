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

// ConsumedMessage pairs a decoded Message with its raw Kafka message so the
// offset can be committed only after a successful database write.
type ConsumedMessage struct {
	Message models.Message
	raw     kafkago.Message
}

// Consumer reads records from a Kafka topic using consumer-group semantics.
type Consumer struct {
	reader *kafkago.Reader
}

// NewConsumer creates a Consumer that joins the given consumer group.
func NewConsumer(brokers []string, topic, groupID string) *Consumer {
	r := kafkago.NewReader(kafkago.ReaderConfig{
		Brokers:        brokers,
		Topic:          topic,
		GroupID:        groupID,
		MinBytes:       1,
		MaxBytes:       10 << 20, // 10 MiB
		StartOffset:    kafkago.FirstOffset,
		// Manual commit — we call CommitMessages after a successful DB write.
		CommitInterval: 0,
	})
	return &Consumer{reader: r}
}

// FetchBatch collects up to batchSize messages, blocking for at most flushInterval.
// It uses FetchMessage (not ReadMessage) to enable explicit offset commits.
func (c *Consumer) FetchBatch(ctx context.Context, batchSize int, flushInterval time.Duration) ([]ConsumedMessage, error) {
	var batch []ConsumedMessage
	deadline := time.Now().Add(flushInterval)

	for len(batch) < batchSize {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			break
		}
		tctx, cancel := context.WithTimeout(ctx, remaining)
		msg, err := c.reader.FetchMessage(tctx)
		cancel()

		if err != nil {
			if ctx.Err() != nil {
				return batch, ctx.Err()
			}
			// Deadline exceeded — flush what we have.
			break
		}

		var m models.Message
		if err := json.Unmarshal(msg.Value, &m); err != nil {
			slog.Warn("skipping malformed message", "offset", msg.Offset, "err", err)
			// Commit the bad message so we don't re-process it forever.
			_ = c.reader.CommitMessages(ctx, msg)
			continue
		}

		batch = append(batch, ConsumedMessage{Message: m, raw: msg})
	}

	return batch, nil
}

// Commit tells Kafka that all messages in the batch have been processed.
func (c *Consumer) Commit(ctx context.Context, batch []ConsumedMessage) error {
	if len(batch) == 0 {
		return nil
	}
	raws := make([]kafkago.Message, len(batch))
	for i, cm := range batch {
		raws[i] = cm.raw
	}
	if err := c.reader.CommitMessages(ctx, raws...); err != nil {
		return fmt.Errorf("commit offsets: %w", err)
	}
	return nil
}

// Close closes the reader.
func (c *Consumer) Close() error {
	return c.reader.Close()
}
