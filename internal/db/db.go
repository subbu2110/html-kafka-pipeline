// Package db manages the database connection and provides idempotent batch inserts.
package db

import (
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"sync"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"

	"html-kafka-pipeline/internal/models"
	"html-kafka-pipeline/internal/schema"
)

// DB wraps a *sql.DB with table-creation tracking.
type DB struct {
	conn          *sql.DB
	dialect       schema.Dialect
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
	d := schema.DialectMySQL
	if driver == "postgres" {
		d = schema.DialectPostgres
	}
	slog.Info("db connected", "driver", driver)
	return &DB{conn: conn, dialect: d, createdTables: make(map[string]struct{})}, nil
}

// EnsureTable creates the target table if it does not yet exist.
// The table includes _id (PK) and _row_hash (UNIQUE) for idempotency.
// It is safe to call concurrently.
func (d *DB) EnsureTable(si models.SchemaInfo) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if _, ok := d.createdTables[si.TableName]; ok {
		return nil
	}

	// Build a schema.Schema from the SchemaInfo so we can use CreateTableSQL.
	sc := &schema.Schema{TableName: si.TableName}
	for _, col := range si.Columns {
		sc.Columns = append(sc.Columns, schema.Column{
			Name: col.Name,
			Type: schema.ColumnType(col.Type),
		})
	}
	ddl := sc.CreateTableSQL(d.dialect)

	if _, err := d.conn.Exec(ddl); err != nil {
		return fmt.Errorf("create table %q: %w", si.TableName, err)
	}

	d.createdTables[si.TableName] = struct{}{}
	slog.Info("table ready", "table", si.TableName)
	return nil
}

// BatchInsert inserts records into tableName inside a single transaction.
// For MySQL it uses INSERT IGNORE; for Postgres it uses ON CONFLICT DO NOTHING.
// Both strategies skip duplicate rows (same _row_hash), providing idempotent writes.
// Returns the number of rows actually inserted.
func (d *DB) BatchInsert(tableName string, columns []models.ColumnInfo, records []map[string]string) (int, error) {
	if len(records) == 0 {
		return 0, nil
	}

	q := d.dialect.QuoteIdent

	colNames := make([]string, len(columns))
	for i, c := range columns {
		colNames[i] = q(c.Name)
	}

	placeholders := make([]string, len(columns)+1) // +1 for _row_hash
	for i := range placeholders {
		if d.dialect == schema.DialectPostgres {
			placeholders[i] = fmt.Sprintf("$%d", i+1)
		} else {
			placeholders[i] = "?"
		}
	}

	var query string
	if d.dialect == schema.DialectPostgres {
		query = fmt.Sprintf(
			"INSERT INTO %s (%s, %s) VALUES (%s) ON CONFLICT (%s) DO NOTHING",
			q(tableName),
			q("_row_hash"),
			strings.Join(colNames, ", "),
			strings.Join(placeholders, ", "),
			q("_row_hash"),
		)
	} else {
		query = fmt.Sprintf(
			"INSERT IGNORE INTO %s (%s, %s) VALUES (%s)",
			q(tableName),
			q("_row_hash"),
			strings.Join(colNames, ", "),
			strings.Join(placeholders, ", "),
		)
	}

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
			slog.Warn("row insert error, skipping", "err", err)
			continue
		}
		n, err := res.RowsAffected()
		if err != nil {
			slog.Warn("RowsAffected error", "err", err)
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
