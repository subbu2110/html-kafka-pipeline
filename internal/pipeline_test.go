// Package pipeline_test is an end-to-end integration test that runs the full
// fetch → parse → schema → message-build pipeline against a live URL.
// It requires internet access but does NOT need Kafka or MySQL.
package pipeline_test

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"html-kafka-pipeline/internal/fetcher"
	"html-kafka-pipeline/internal/models"
	"html-kafka-pipeline/internal/parser"
	"html-kafka-pipeline/internal/schema"
)

// testCase describes one URL to validate end-to-end.
type testCase struct {
	name          string
	url           string
	cssClass      string
	tableIndex    int
	minRows       int // minimum expected data rows
	minCols       int // minimum expected columns
	wantBigInt    []string // column names expected to be BIGINT
	wantDouble    []string // column names expected to be DOUBLE
	wantVarchar   []string // column names expected to be VARCHAR or TEXT
}

var cases = []testCase{
	{
		name:       "Wikipedia countries by population",
		url:        "https://en.wikipedia.org/wiki/List_of_countries_by_population_(United_Nations)",
		cssClass:   "wikitable",
		minRows:    50,
		minCols:    4,
		wantBigInt: []string{"population_1_july_2023"},
	},
	{
		name:       "Wikipedia list of largest cities",
		url:        "https://en.wikipedia.org/wiki/List_of_largest_cities",
		cssClass:   "wikitable",
		minRows:    20,
		minCols:    3,
	},
	{
		name:       "Wikipedia sovereign states by area",
		url:        "https://en.wikipedia.org/wiki/List_of_countries_and_dependencies_by_area",
		cssClass:   "wikitable",
		minRows:    30,
		minCols:    3,
	},
}

func TestPipeline_LiveFetch(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping live fetch test in short mode")
	}

	f := fetcher.New(30*time.Second, 2)

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			// ── 1. Fetch ──────────────────────────────────────────────────
			body, err := f.Fetch(tc.url)
			if err != nil {
				t.Fatalf("fetch failed: %v", err)
			}
			defer body.Close()

			// ── 2. Parse ──────────────────────────────────────────────────
			tbl, err := parser.Parse(body, parser.ParseOptions{
				CSSClass:   tc.cssClass,
				TableIndex: tc.tableIndex,
			})
			if err != nil {
				t.Fatalf("parse failed: %v", err)
			}

			if len(tbl.Headers) < tc.minCols {
				t.Errorf("got %d columns, want at least %d; headers: %v",
					len(tbl.Headers), tc.minCols, tbl.Headers)
			}
			if len(tbl.Rows) < tc.minRows {
				t.Errorf("got %d rows, want at least %d", len(tbl.Rows), tc.minRows)
			}
			t.Logf("columns (%d): %v", len(tbl.Headers), tbl.Headers)
			t.Logf("rows:    %d", len(tbl.Rows))

			// ── 3. Schema inference ───────────────────────────────────────
			sc := schema.Infer("test_table", tbl.Headers, tbl.Rows)

			if len(sc.Columns) == 0 {
				t.Fatal("schema has zero columns")
			}

			// Column names must all be unique (no DDL collision).
			seen := map[string]bool{}
			for _, col := range sc.Columns {
				if seen[col.Name] {
					t.Errorf("duplicate column name %q in inferred schema", col.Name)
				}
				seen[col.Name] = true
				if len(col.Name) > 64 {
					t.Errorf("column name %q exceeds MySQL 64-char limit", col.Name)
				}
			}

			// Verify expected BIGINT columns.
			colMap := map[string]schema.ColumnType{}
			for _, col := range sc.Columns {
				colMap[col.Name] = col.Type
			}
			for _, name := range tc.wantBigInt {
				if typ, ok := colMap[name]; !ok {
					t.Errorf("expected column %q not found; got: %v", name, sc.Columns)
				} else if typ != schema.TypeBigInt {
					t.Errorf("column %q: got type %q, want BIGINT", name, typ)
				}
			}

			// Validate CREATE TABLE DDL.
			ddl := sc.CreateTableSQL(schema.DialectMySQL)
			for _, must := range []string{
				"CREATE TABLE IF NOT EXISTS",
				"`_id` BIGINT AUTO_INCREMENT PRIMARY KEY",
				"`_row_hash` VARCHAR(64)",
			} {
				if !strings.Contains(ddl, must) {
					t.Errorf("DDL missing %q", must)
				}
			}

			// ── 4. Build Kafka messages ───────────────────────────────────
			schemaInfo := models.SchemaInfo{TableName: sc.TableName}
			for _, col := range sc.Columns {
				schemaInfo.Columns = append(schemaInfo.Columns, models.ColumnInfo{
					Name: col.Name,
					Type: string(col.Type),
				})
			}

			var messages []models.Message
			for _, row := range tbl.Rows {
				rec := map[string]string{}
				for i, col := range sc.Columns {
					if i < len(row) {
						rec[col.Name] = schema.CleanValue(row[i])
					}
				}
				messages = append(messages, models.Message{Schema: schemaInfo, Record: rec})
			}

			if len(messages) == 0 {
				t.Fatal("no messages produced")
			}

			// ── 5. Validate JSON serialization ────────────────────────────
			for i, msg := range messages[:min(3, len(messages))] {
				b, err := json.Marshal(msg)
				if err != nil {
					t.Fatalf("message %d: marshal failed: %v", i, err)
				}
				var roundtrip models.Message
				if err := json.Unmarshal(b, &roundtrip); err != nil {
					t.Fatalf("message %d: unmarshal failed: %v", i, err)
				}
				if roundtrip.Schema.TableName != msg.Schema.TableName {
					t.Errorf("message %d: table name roundtrip mismatch", i)
				}
				if len(roundtrip.Record) != len(msg.Record) {
					t.Errorf("message %d: record field count mismatch after roundtrip", i)
				}
			}

			t.Logf("sample message[0]: %s", func() string {
				b, _ := json.MarshalIndent(messages[0], "", "  ")
				return string(b)
			}())
		})
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
