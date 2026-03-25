# HTML → Kafka → MySQL Pipeline

A production-oriented Go service that fetches an HTML table from any URL, infers its schema, streams every row through Kafka, and persists the data into MySQL — all driven by configuration with no code changes required.

---

## Architecture

```
Target URL
    │  HTTP GET (retry + backoff)
    ▼
 Fetcher  ──►  Parser  ──►  Schema Inference
                                   │
                            Kafka Producer  ──►  Kafka Topic
                                                      │
                                               Kafka Consumer
                                                      │
                                                   MySQL
                                          (dynamic DDL + batch insert)
```

**Flow:**
1. **Fetcher** — HTTP GET with exponential-backoff retry, 50 MiB response cap
2. **Parser** — extracts headers and rows from the selected `<table>`, handling `colspan` and `rowspan`
3. **Schema inference** — assigns the most specific SQL type per column (`BIGINT → DOUBLE → DATE → VARCHAR → TEXT`), normalising currency symbols, commas, `%` suffixes, and footnote markers
4. **Producer** — serialises each row as a JSON message (schema embedded) and batch-publishes to Kafka
5. **Consumer** — reads batches, creates the MySQL table on first message, and batch-inserts with `INSERT IGNORE` for idempotency

---

## Quick start

```bash
cp .env.example .env          # fill in DB_DSN (required)
make up                        # start Kafka + MySQL via Docker Compose
make consumer                  # terminal A — long-running consumer
make producer                  # terminal B — one-shot fetch + publish
```

---

## Configuration

All values via environment variables or `.env` file. No credentials are hardcoded.

| Variable | Default | Description |
|---|---|---|
| `TARGET_URL` | Wikipedia countries by population | Page to fetch |
| `TABLE_CSS_CLASS` | `wikitable` | Prefer table with this CSS class |
| `TABLE_INDEX` | `0` | Fallback: 0-based table index |
| `TABLE_NAME` | `html_data` | MySQL destination table name |
| `KAFKA_BROKERS` | `localhost:9092` | Comma-separated brokers |
| `KAFKA_TOPIC` | `html-table-records` | Topic name |
| `KAFKA_GROUP_ID` | `html-pipeline-consumer` | Consumer group |
| `DB_DRIVER` | `mysql` | Database driver |
| `DB_DSN` | **required** | e.g. `user:pass@tcp(host:3306)/db?parseTime=true` |
| `FETCH_TIMEOUT_SEC` | `30` | HTTP timeout in seconds |
| `FETCH_RETRIES` | `3` | Max retry attempts |
| `BATCH_SIZE` | `100` | Rows per Kafka batch / DB insert |
| `BATCH_FLUSH_MS` | `5000` | Consumer flush interval (ms) |

---

## Design decisions

### Idempotent writes
Every Kafka message carries a `_row_hash` (SHA-256 of the record JSON). The MySQL table has a `UNIQUE` constraint on this column and inserts use `INSERT IGNORE` — re-running the producer never creates duplicates.

### At-least-once delivery
The consumer uses `FetchMessage` + explicit `CommitMessages` (not auto-commit). Offsets are committed after each successful DB batch. If the process crashes mid-flight, Kafka re-delivers the same messages, which are safely deduplicated by `_row_hash`.

### Schema embedded in messages
Each Kafka message carries the full `SchemaInfo` alongside the record. The consumer can create the target MySQL table on first delivery with no separate coordination or schema registry.

### Type inference hierarchy
```
BIGINT → DOUBLE → DATE → VARCHAR(512) → TEXT
```
Each column starts as BIGINT and widens only when a value is incompatible. Currency symbols, thousands separators, and `%` suffixes are stripped before parsing. Duplicate column names after sanitisation are deduped (`change`, `change_2`, …).

---

## Project structure

```
cmd/
  producer/   fetch → parse → infer → publish to Kafka
  consumer/   read Kafka → ensure table → batch insert to MySQL
internal/
  fetcher/    HTTP client with retry/backoff
  parser/     HTML table extractor (colspan, rowspan, CSS class selection)
  schema/     type inference, name sanitisation, DDL generation
  kafka/      producer and consumer wrappers
  db/         dynamic DDL, idempotent batch inserts
  models/     shared Message / SchemaInfo / ColumnInfo types
config/       environment-variable loader
```

---

## Running tests

```bash
# Unit tests — no network or Docker required
go test ./internal/... -short

# Full end-to-end tests — fetches real URLs (requires internet)
go test ./internal/... -v -timeout 120s
```

---

## Known limitations

- **JavaScript-rendered tables** — plain HTTP GET only; pages that build tables via JS (Yahoo Finance, Worldometers) are not supported
- **Multi-row headers** — only the first `<th>` row is used as the header; hierarchical/merged headers are treated as data rows
- **Schema evolution** — if the source table gains or loses columns between runs, `CREATE TABLE IF NOT EXISTS` keeps the original schema; mismatched rows are logged and skipped
- **Non-UTF-8 pages** — charset detection is not implemented
