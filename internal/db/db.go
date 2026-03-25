// Package db manages the MySQL connection and provides idempotent batch inserts.
package db

import (
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"sync"

	_ "github.com/go-sql-driver/mysql"

	"html-kafka-pipeline/internal/models"
)

// DB wraps a *sql.DB with table-creation tracking.
type DB struct {
	conn          *sql.DB
	mu            sync.Mutex
	createdTables map[string]struct{}
}

// New opens a database connection and verifies it with a ping.
func New(driver, dsn string) (*DB, error) {
	conn, err := sql.Open(driver, dsn)
	if err != nil {
		return nil, fmt.Errorf("open db: %w", err)
	}
	conn.SetMaxOpenConns(10)
	conn.SetMaxIdleConns(5)
	if err := conn.Ping(); err != nil {
		return nil, fmt.Errorf("ping db: %w", err)
	}
	log.Printf("[db] connected to %s", driver)
	return &DB{conn: conn, createdTables: make(map[string]struct{})}, nil
}

// EnsureTable creates the target table if it does not yet exist.
// The table includes _id (PK) and _row_hash (UNIQUE) for idempotency.
// It is safe to call concurrently.
func (d *DB) EnsureTable(schema models.SchemaInfo) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if _, ok := d.createdTables[schema.TableName]; ok {
		return nil
	}

	parts := []string{
		"`_id` BIGINT AUTO_INCREMENT PRIMARY KEY",
		"`_row_hash` VARCHAR(64) NOT NULL UNIQUE",
	}
	for _, col := range schema.Columns {
		parts = append(parts, fmt.Sprintf("`%s` %s", col.Name, col.Type))
	}

	ddl := fmt.Sprintf(
		"CREATE TABLE IF NOT EXISTS `%s` (\n  %s\n)",
		schema.TableName,
		strings.Join(parts, ",\n  "),
	)

	if _, err := d.conn.Exec(ddl); err != nil {
		return fmt.Errorf("create table %q: %w", schema.TableName, err)
	}

	d.createdTables[schema.TableName] = struct{}{}
	log.Printf("[db] table %q ready", schema.TableName)
	return nil
}

// BatchInsert inserts records into tableName inside a single transaction.
// It uses INSERT IGNORE so duplicate rows (same _row_hash) are silently skipped,
// providing at-least-once delivery with idempotent writes.
// Returns the number of rows actually inserted.
func (d *DB) BatchInsert(tableName string, columns []models.ColumnInfo, records []map[string]string) (int, error) {
	if len(records) == 0 {
		return 0, nil
	}

	colNames := make([]string, len(columns))
	for i, c := range columns {
		colNames[i] = "`" + c.Name + "`"
	}

	// INSERT IGNORE silently discards rows that violate the UNIQUE constraint on _row_hash.
	placeholders := make([]string, len(columns)+1) // +1 for _row_hash
	for i := range placeholders {
		placeholders[i] = "?"
	}
	query := fmt.Sprintf(
		"INSERT IGNORE INTO `%s` (`_row_hash`, %s) VALUES (%s)",
		tableName,
		strings.Join(colNames, ", "),
		strings.Join(placeholders, ", "),
	)

	tx, err := d.conn.Begin()
	if err != nil {
		return 0, fmt.Errorf("begin transaction: %w", err)
	}
	defer tx.Rollback() //nolint:errcheck

	stmt, err := tx.Prepare(query)
	if err != nil {
		return 0, fmt.Errorf("prepare statement: %w", err)
	}
	defer stmt.Close()

	inserted := 0
	for _, rec := range records {
		hash := rowHash(rec)
		args := make([]interface{}, len(columns)+1)
		args[0] = hash
		for i, col := range columns {
			v := rec[col.Name]
			if v == "" {
				args[i+1] = nil
			} else {
				args[i+1] = v
			}
		}

		res, err := stmt.Exec(args...)
		if err != nil {
			// Log and continue — one bad row must not abort the whole batch.
			log.Printf("[db] row insert error (skipping): %v", err)
			continue
		}
		n, err := res.RowsAffected()
		if err != nil {
			log.Printf("[db] RowsAffected error: %v", err)
		}
		inserted += int(n)
	}

	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("commit transaction: %w", err)
	}
	return inserted, nil
}

// rowHash produces a deterministic 64-character hex string from the record's
// JSON representation, used as a deduplication key.
func rowHash(rec map[string]string) string {
	b, _ := json.Marshal(rec)
	sum := sha256.Sum256(b)
	return fmt.Sprintf("%x", sum)
}

// Close releases the database connection pool.
func (d *DB) Close() error {
	return d.conn.Close()
}
