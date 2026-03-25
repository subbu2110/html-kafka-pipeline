package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config holds all runtime configuration, loaded from environment variables.
type Config struct {
	URL           string
	TableIndex    int
	TableCSSClass string
	TableName     string
	KafkaBrokers  []string
	KafkaTopic    string
	KafkaGroupID  string
	DBDriver      string
	DBDSN         string
	FetchTimeout  time.Duration
	FetchRetries  int
	BatchSize     int
	BatchFlushMs  int
}

// Load reads configuration from environment variables.
// DB_DSN and KAFKA_BROKERS are required — the application exits immediately
// if they are absent, so credentials are never hardcoded.
func Load() *Config {
	return &Config{
		URL:           getEnv("TARGET_URL", "https://en.wikipedia.org/wiki/List_of_countries_by_population_(United_Nations)"),
		TableIndex:    getInt("TABLE_INDEX", 0),
		TableCSSClass: getEnv("TABLE_CSS_CLASS", "wikitable"),
		TableName:     getEnv("TABLE_NAME", "html_data"),
		KafkaBrokers:  splitEnv("KAFKA_BROKERS", "localhost:9092"),
		KafkaTopic:    getEnv("KAFKA_TOPIC", "html-table-records"),
		KafkaGroupID:  getEnv("KAFKA_GROUP_ID", "html-pipeline-consumer"),
		DBDriver:      getEnv("DB_DRIVER", "mysql"),
		DBDSN:         mustEnv("DB_DSN"),
		FetchTimeout:  time.Duration(getInt("FETCH_TIMEOUT_SEC", 30)) * time.Second,
		FetchRetries:  getInt("FETCH_RETRIES", 3),
		BatchSize:     getInt("BATCH_SIZE", 100),
		BatchFlushMs:  getInt("BATCH_FLUSH_MS", 5000),
	}
}

// mustEnv returns the value of key or panics with a clear message.
// Used for credentials that must never have a hardcoded fallback.
func mustEnv(key string) string {
	v := os.Getenv(key)
	if v == "" {
		panic(fmt.Sprintf("required environment variable %q is not set — set it in .env or the environment", key))
	}
	return v
}

func getEnv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func getInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func splitEnv(key, def string) []string {
	v := getEnv(key, def)
	parts := strings.Split(v, ",")
	for i := range parts {
		parts[i] = strings.TrimSpace(parts[i])
	}
	return parts
}
