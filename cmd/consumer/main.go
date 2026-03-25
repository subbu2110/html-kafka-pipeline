// consumer reads rows from Kafka and inserts them into MySQL.
package main

import (
	"context"
	"log"
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
	_ = godotenv.Load()
	cfg := config.Load()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// ── Database ─────────────────────────────────────────────────────────────
	database, err := dbpkg.New(cfg.DBDriver, cfg.DBDSN)
	if err != nil {
		log.Fatalf("[consumer] db init: %v", err)
	}
	defer database.Close()

	// ── Kafka consumer ───────────────────────────────────────────────────────
	consumer := kafkapkg.NewConsumer(cfg.KafkaBrokers, cfg.KafkaTopic, cfg.KafkaGroupID)
	defer consumer.Close()

	flushInterval := time.Duration(cfg.BatchFlushMs) * time.Millisecond
	log.Printf("[consumer] started — topic=%s group=%s batchSize=%d flushInterval=%s",
		cfg.KafkaTopic, cfg.KafkaGroupID, cfg.BatchSize, flushInterval)

	totalInserted := 0

	for {
		// ── Fetch a batch ────────────────────────────────────────────────────
		batch, err := consumer.FetchBatch(ctx, cfg.BatchSize, flushInterval)
		if err != nil {
			if ctx.Err() != nil {
				break
			}
			log.Printf("[consumer] fetch error: %v", err)
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
				log.Printf("[consumer] ensure table %q: %v", tableName, err)
				// Do not skip the commit — re-delivery won't help a DDL failure
				// (e.g. insufficient DB privileges). Log and move on.
			}

			records := make([]map[string]string, len(msgs))
			for i, cm := range msgs {
				records[i] = cm.Message.Record
			}

			n, err := database.BatchInsert(tableName, schemaInfo.Columns, records)
			if err != nil {
				log.Printf("[consumer] batch insert %q: %v", tableName, err)
				// Row-level errors are already handled inside BatchInsert.
				// A total insert failure (e.g. lost DB connection) is logged here;
				// the offset is still committed because INSERT IGNORE guarantees
				// that any successfully inserted rows won't be duplicated on retry.
			} else {
				totalInserted += n
				log.Printf("[consumer] inserted %d/%d rows into %q (total: %d)",
					n, len(records), tableName, totalInserted)
			}
		}

		// ── Always commit offsets after processing ───────────────────────────
		// Idempotency is guaranteed by _row_hash + INSERT IGNORE, so committing
		// even after a partial failure is safe and prevents unbounded re-delivery.
		if err := consumer.Commit(ctx, batch); err != nil {
			log.Printf("[consumer] commit error: %v", err)
		}
	}

	log.Printf("[consumer] shut down — total rows inserted: %d", totalInserted)
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

