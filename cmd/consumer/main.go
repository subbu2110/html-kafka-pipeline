// consumer reads rows from Kafka and inserts them into MySQL.
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/joho/godotenv"

	"html-kafka-pipeline/config"
	dbpkg "html-kafka-pipeline/internal/db"
	kafkapkg "html-kafka-pipeline/internal/kafka"
)

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))

	_ = godotenv.Load()
	cfg := config.Load()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// ── Database ─────────────────────────────────────────────────────────────
	database, err := dbpkg.New(cfg.DBDriver, cfg.DBDSN)
	if err != nil {
		slog.Error("db init failed", "err", err)
		os.Exit(1)
	}
	defer database.Close()

	// ── Kafka consumer ───────────────────────────────────────────────────────
	consumer := kafkapkg.NewConsumer(cfg.KafkaBrokers, cfg.KafkaTopic, cfg.KafkaGroupID)
	defer consumer.Close()

	flushInterval := time.Duration(cfg.BatchFlushMs) * time.Millisecond
	slog.Info("consumer started", "topic", cfg.KafkaTopic, "group", cfg.KafkaGroupID,
		"batchSize", cfg.BatchSize, "flushInterval", flushInterval)

	totalInserted := 0

	for {
		// ── Fetch a batch ────────────────────────────────────────────────────
		batch, err := consumer.FetchBatch(ctx, cfg.BatchSize, flushInterval)
		if err != nil {
			if ctx.Err() != nil {
				break
			}
			slog.Warn("fetch error", "err", err)
			continue
		}
		if len(batch) == 0 {
			if ctx.Err() != nil {
				break
			}
			continue
		}

		// ── Group by table name ──────────────────────────────────────────────
		grouped := groupByTable(batch)

		// ── Persist each group ───────────────────────────────────────────────
		for tableName, msgs := range grouped {
			schemaInfo := msgs[0].Message.Schema

			if err := database.EnsureTable(schemaInfo); err != nil {
				slog.Error("ensure table failed", "table", tableName, "err", err)
				// Do not skip the commit — re-delivery won't help a DDL failure
				// (e.g. insufficient DB privileges). Log and move on.
			}

			records := make([]map[string]string, len(msgs))
			for i, cm := range msgs {
				records[i] = cm.Message.Record
			}

			n, err := database.BatchInsert(tableName, schemaInfo.Columns, records)
			if err != nil {
				slog.Error("batch insert failed", "table", tableName, "err", err)
			} else {
				totalInserted += n
				slog.Info("batch inserted", "table", tableName, "inserted", n, "batch", len(records), "total", totalInserted)
			}
		}

		// ── Always commit offsets after processing ───────────────────────────
		// Idempotency is guaranteed by _row_hash + INSERT IGNORE / ON CONFLICT,
		// so committing even after a partial failure is safe.
		if err := consumer.Commit(ctx, batch); err != nil {
			slog.Warn("offset commit failed", "err", err)
		}
	}

	slog.Info("consumer shut down", "totalInserted", totalInserted)
}

// groupByTable splits a batch of ConsumedMessages by their target table name.
func groupByTable(batch []kafkapkg.ConsumedMessage) map[string][]kafkapkg.ConsumedMessage {
	m := make(map[string][]kafkapkg.ConsumedMessage)
	for _, cm := range batch {
		key := cm.Message.Schema.TableName
		m[key] = append(m[key], cm)
	}
	return m
}

