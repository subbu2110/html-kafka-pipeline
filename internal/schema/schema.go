// Package schema infers SQL column types from raw string data and produces DDL.
package schema

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// ColumnType is a SQL column type string (compatible with MySQL and Postgres).
type ColumnType string

const (
	TypeBigInt    ColumnType = "BIGINT"
	TypeDouble    ColumnType = "DOUBLE"
	TypeDate      ColumnType = "DATE"
	TypeTimestamp ColumnType = "TIMESTAMP"
	TypeVarchar   ColumnType = "VARCHAR(512)"
	TypeText      ColumnType = "TEXT"
)

// Column pairs an inferred type with a sanitised column name.
type Column struct {
	Name string
	Type ColumnType
}

// Schema describes the inferred structure of an HTML table.
type Schema struct {
	TableName string
	Columns   []Column
}

// timestampFormats lists formats that include a time component → TIMESTAMP.
var timestampFormats = []string{
	"2006-01-02 15:04:05",
	"2006-01-02T15:04:05Z",
	"2006-01-02T15:04:05",
	"2006-01-02T15:04:05-07:00",
	"01/02/2006 15:04:05",
	"Jan 2, 2006 15:04:05",
}

// dateFormats lists date-only formats → DATE.
// "2006" (year-only) is intentionally excluded — a bare 4-digit year is more
// naturally a BIGINT and the format is too broad (every integer 1000–9999 matches).
var dateFormats = []string{
	"2006-01-02",
	"01/02/2006",
	"2/1/2006",
	"January 2, 2006",
	"Jan 2, 2006",
	"Jan. 2, 2006",
}

// footnoteRE strips Wikipedia-style footnote markers like [1], [a].
var footnoteRE = regexp.MustCompile(`\[[^\]]*\]`)

// Infer examines all rows for each column and picks the most specific type
// that is still consistent across every non-empty cell.
func Infer(tableName string, headers []string, rows [][]string) *Schema {
	n := len(headers)
	types := make([]ColumnType, n)
	for i := range types {
		types[i] = TypeBigInt // start optimistic
	}

	for _, row := range rows {
		for i := 0; i < n && i < len(row); i++ {
			v := CleanValue(row[i])
			if v == "" {
				continue // treat empty as compatible with any type
			}
			types[i] = coerce(types[i], v)
		}
	}

	cols := make([]Column, n)
	for i, h := range headers {
		cols[i] = Column{
			Name: SanitiseName(h),
			Type: types[i],
		}
	}

	// Deduplicate column names produced by sanitization.
	// e.g. "% change" and "$ change" both become "change"; the second gets "change_2".
	seen := make(map[string]int, n)
	for i := range cols {
		base := cols[i].Name
		if count, exists := seen[base]; exists {
			count++
			seen[base] = count
			candidate := fmt.Sprintf("%s_%d", base, count)
			if len(candidate) > 64 {
				candidate = candidate[:64]
			}
			cols[i].Name = candidate
		} else {
			seen[base] = 1
		}
	}

	return &Schema{
		TableName: SanitiseName(tableName),
		Columns:   cols,
	}
}

// coerce narrows the current inferred type when val is incompatible with it.
// The type hierarchy is: BIGINT → DOUBLE → TIMESTAMP → DATE → VARCHAR → TEXT
func coerce(current ColumnType, val string) ColumnType {
	switch current {
	case TypeBigInt:
		if isInt(val) {
			return TypeBigInt
		}
		fallthrough
	case TypeDouble:
		if isFloat(val) {
			return TypeDouble
		}
		fallthrough
	case TypeTimestamp:
		if isTimestamp(val) {
			return TypeTimestamp
		}
		fallthrough
	case TypeDate:
		if isDate(val) {
			return TypeDate
		}
		if len(val) > 512 {
			return TypeText
		}
		return TypeVarchar
	default:
		if len(val) > 512 {
			return TypeText
		}
		return TypeVarchar
	}
}

// stripNumericDecorations removes common currency symbols and percent signs
// so that values like "$1,234", "€99.50", and "12.5%" can be parsed as numbers.
func stripNumericDecorations(s string) string {
	s = strings.TrimPrefix(s, "$")
	s = strings.TrimPrefix(s, "€")
	s = strings.TrimPrefix(s, "£")
	s = strings.TrimPrefix(s, "¥")
	s = strings.TrimPrefix(s, "₹")
	s = strings.TrimSuffix(s, "%")
	return s
}

func isInt(s string) bool {
	s = stripNumericDecorations(s)
	s = strings.ReplaceAll(s, ",", "")
	s = strings.TrimPrefix(s, "+")
	_, err := strconv.ParseInt(s, 10, 64)
	return err == nil
}

func isFloat(s string) bool {
	s = stripNumericDecorations(s)
	s = strings.ReplaceAll(s, ",", "")
	s = strings.TrimPrefix(s, "+")
	_, err := strconv.ParseFloat(s, 64)
	return err == nil
}

func isTimestamp(s string) bool {
	for _, f := range timestampFormats {
		if _, err := time.Parse(f, s); err == nil {
			return true
		}
	}
	return false
}

func isDate(s string) bool {
	for _, f := range dateFormats {
		if _, err := time.Parse(f, s); err == nil {
			return true
		}
	}
	return false
}

// CleanValue removes footnote markers and trims whitespace.
// Exported so the producer can apply the same cleaning logic.
func CleanValue(s string) string {
	s = footnoteRE.ReplaceAllString(s, "")
	s = strings.TrimSpace(s)
	// Normalise common "no data" tokens to empty string.
	switch s {
	case "—", "–", "-", "N/A", "n/a", "NA", "?":
		return ""
	}
	return s
}

// SanitiseName converts a human-readable header to a valid SQL identifier.
func SanitiseName(s string) string {
	s = footnoteRE.ReplaceAllString(s, "")
	s = strings.TrimSpace(s)
	re := regexp.MustCompile(`[^a-zA-Z0-9]+`)
	s = re.ReplaceAllString(s, "_")
	s = strings.Trim(s, "_")
	s = strings.ToLower(s)
	if s == "" {
		s = "col"
	}
	// Avoid SQL reserved words that are common column names.
	reserved := map[string]bool{"rank": true, "index": true, "order": true, "key": true, "group": true}
	if reserved[s] {
		s = s + "_"
	}
	// MySQL column names must not exceed 64 characters.
	if len(s) > 64 {
		s = s[:64]
	}
	return s
}

// Dialect selects database-specific SQL syntax.
type Dialect string

const (
	DialectMySQL    Dialect = "mysql"
	DialectPostgres Dialect = "postgres"
)

// QuoteIdent wraps an identifier in the dialect-appropriate quoting character.
func (d Dialect) QuoteIdent(name string) string {
	if d == DialectPostgres {
		return `"` + name + `"`
	}
	return "`" + name + "`"
}

// quote is an unexported alias used within the package.
func (d Dialect) quote(name string) string { return d.QuoteIdent(name) }

// CreateTableSQL returns a CREATE TABLE IF NOT EXISTS statement for the schema.
// It adds _id (PK) and _row_hash (UNIQUE) columns for idempotent inserts.
// Pass DialectMySQL or DialectPostgres to get the appropriate DDL.
func (s *Schema) CreateTableSQL(dialect Dialect) string {
	q := dialect.quote

	var idCol string
	if dialect == DialectPostgres {
		idCol = fmt.Sprintf("%s BIGSERIAL PRIMARY KEY", q("_id"))
	} else {
		idCol = fmt.Sprintf("%s BIGINT AUTO_INCREMENT PRIMARY KEY", q("_id"))
	}

	parts := []string{
		idCol,
		fmt.Sprintf("%s VARCHAR(64) NOT NULL UNIQUE", q("_row_hash")),
	}
	for _, col := range s.Columns {
		parts = append(parts, fmt.Sprintf("%s %s", q(col.Name), col.Type))
	}
	return fmt.Sprintf(
		"CREATE TABLE IF NOT EXISTS %s (\n  %s\n)",
		q(s.TableName),
		strings.Join(parts, ",\n  "),
	)
}
