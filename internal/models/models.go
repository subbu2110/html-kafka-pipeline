// Package models defines the shared data structures passed between the Kafka
// producer and consumer so that neither package depends on the other.
package models

// Message is the JSON payload published to Kafka for every extracted row.
type Message struct {
	Schema SchemaInfo        `json:"schema"`
	Record map[string]string `json:"record"`
}

// SchemaInfo carries the inferred table schema alongside every record so the
// consumer can create the target table without a separate coordination step.
type SchemaInfo struct {
	TableName string       `json:"table_name"`
	Columns   []ColumnInfo `json:"columns"`
}

// ColumnInfo describes a single column.
type ColumnInfo struct {
	Name string `json:"name"`
	Type string `json:"type"`
}
