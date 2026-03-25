// producer fetches an HTML table, infers its schema, and publishes every row
// as a JSON message to Kafka.
package main

import (
	"context"
	"log/slog"
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
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))

	// Load .env if present; ignore error when the file doesn't exist.
	_ = godotenv.Load()
	cfg := config.Load()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// ── 1. Fetch HTML ────────────────────────────────────────────────────────
	slog.Info("fetching URL", "url", cfg.URL)
	f := fetcher.New(cfg.FetchTimeout, cfg.FetchRetries)
	body, err := f.Fetch(cfg.URL)
	if err != nil {
		slog.Error("fetch failed", "err", err)
		os.Exit(1)
	}
	defer body.Close()

	// ── 2. Parse HTML table ──────────────────────────────────────────────────
	table, err := parser.Parse(body, parser.ParseOptions{
		TableIndex: cfg.TableIndex,
		CSSClass:   cfg.TableCSSClass,
	})
	if err != nil {
		slog.Error("parse failed", "err", err)
		os.Exit(1)
	}
	slog.Info("parsed table", "columns", len(table.Headers), "rows", len(table.Rows))

	// ── 3. Infer schema ──────────────────────────────────────────────────────
	sc := schema.Infer(cfg.TableName, table.Headers, table.Rows)
	if len(sc.Columns) == 0 {
		slog.Error("table has no columns — check TABLE_INDEX / TABLE_CSS_CLASS", "table", sc.TableName)
		os.Exit(1)
	}
	slog.Info("inferred schema", "table", sc.TableName, "columns", len(sc.Columns))
	for _, col := range sc.Columns {
		slog.Debug("column", "name", col.Name, "type", col.Type)
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
			slog.Warn("producer close error", "err", err)
		}
	}()

	total := 0
	for i := 0; i < len(messages); i += cfg.BatchSize {
		end := i + cfg.BatchSize
		if end > len(messages) {
			end = len(messages)
		}
		if err := producer.Publish(ctx, messages[i:end]); err != nil {
			slog.Error("publish failed", "err", err)
			os.Exit(1)
		}
		total += end - i
	}

	slog.Info("producer done", "published", total, "total", len(messages), "topic", cfg.KafkaTopic)
}
