// producer fetches an HTML table, infers its schema, and publishes every row
// as a JSON message to Kafka.
package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/joho/godotenv"

	"html-kafka-pipeline/config"
	"html-kafka-pipeline/internal/fetcher"
	kafkapkg "html-kafka-pipeline/internal/kafka"
	"html-kafka-pipeline/internal/models"
	"html-kafka-pipeline/internal/parser"
	"html-kafka-pipeline/internal/schema"
)

func main() {
	// Load .env if present; ignore error when the file doesn't exist.
	_ = godotenv.Load()
	cfg := config.Load()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// ── 1. Fetch HTML ────────────────────────────────────────────────────────
	log.Printf("[producer] fetching %s", cfg.URL)
	f := fetcher.New(cfg.FetchTimeout, cfg.FetchRetries)
	body, err := f.Fetch(cfg.URL)
	if err != nil {
		log.Fatalf("[producer] fetch error: %v", err)
	}
	defer body.Close()

	// ── 2. Parse HTML table ──────────────────────────────────────────────────
	table, err := parser.Parse(body, parser.ParseOptions{
		TableIndex: cfg.TableIndex,
		CSSClass:   cfg.TableCSSClass,
	})
	if err != nil {
		log.Fatalf("[producer] parse error: %v", err)
	}
	log.Printf("[producer] parsed table: %d columns, %d rows", len(table.Headers), len(table.Rows))

	// ── 3. Infer schema ──────────────────────────────────────────────────────
	sc := schema.Infer(cfg.TableName, table.Headers, table.Rows)
	if len(sc.Columns) == 0 {
		log.Fatalf("[producer] table %q has no columns — check TABLE_INDEX / TABLE_CSS_CLASS config", sc.TableName)
	}
	log.Printf("[producer] inferred schema for table %q:", sc.TableName)
	for _, col := range sc.Columns {
		log.Printf("  %-30s %s", col.Name, col.Type)
	}

	// Build the SchemaInfo that will be embedded in every Kafka message.
	schemaInfo := models.SchemaInfo{
		TableName: sc.TableName,
		Columns:   make([]models.ColumnInfo, len(sc.Columns)),
	}
	for i, col := range sc.Columns {
		schemaInfo.Columns[i] = models.ColumnInfo{
			Name: col.Name,
			Type: string(col.Type),
		}
	}

	// ── 4. Build Kafka messages ──────────────────────────────────────────────
	messages := make([]models.Message, 0, len(table.Rows))
	for _, row := range table.Rows {
		rec := make(map[string]string, len(sc.Columns))
		for i, col := range sc.Columns {
			if i < len(row) {
				rec[col.Name] = schema.CleanValue(row[i])
			}
		}
		messages = append(messages, models.Message{
			Schema: schemaInfo,
			Record: rec,
		})
	}

	// ── 5. Publish to Kafka in batches ───────────────────────────────────────
	producer := kafkapkg.NewProducer(cfg.KafkaBrokers, cfg.KafkaTopic)
	defer func() {
		if err := producer.Close(); err != nil {
			log.Printf("[producer] close error: %v", err)
		}
	}()

	total := 0
	for i := 0; i < len(messages); i += cfg.BatchSize {
		end := i + cfg.BatchSize
		if end > len(messages) {
			end = len(messages)
		}
		if err := producer.Publish(ctx, messages[i:end]); err != nil {
			log.Fatalf("[producer] publish error: %v", err)
		}
		total += end - i
	}

	log.Printf("[producer] done — published %d/%d records to topic %q", total, len(messages), cfg.KafkaTopic)
}
