# HTML → Kafka → MySQL Pipeline

A Go service that scrapes an HTML table from any URL, infers its schema automatically, streams every row through Kafka, and writes the data into MySQL (or Postgres). Everything is configured via environment variables — no code changes needed to point it at a different page or database.

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
                                              MySQL / Postgres
                                          (dynamic DDL + batch insert)
```

**Flow:**
1. **Fetcher** — HTTP GET with exponential-backoff retry and a 50 MiB response cap
2. **Parser** — extracts headers and rows from the target `<table>`, correctly handling `colspan` and `rowspan`
3. **Schema inference** — picks the most specific SQL type per column (`BIGINT → DOUBLE → TIMESTAMP → DATE → VARCHAR → TEXT`), stripping currency symbols, commas, `%` suffixes, and Wikipedia-style footnote markers before type-checking
4. **Producer** — serializes each row as a JSON message with the schema embedded, then batch-publishes to Kafka
5. **Consumer** — reads batches from Kafka, creates the destination table on the first message, and batch-inserts rows with deduplication

---

## Quick start

```bash
cp .env.example .env          # fill in DB_DSN (required)
make up                       # start Kafka + MySQL via Docker Compose
make consumer                 # terminal A — long-running consumer
make producer                 # terminal B — one-shot fetch + publish
```

---

## Configuration

All values come from environment variables or a `.env` file. No credentials are hardcoded anywhere.

| Variable | Default | Description |
|---|---|---|
| `TARGET_URL` | Wikipedia countries by population | Page to scrape |
| `TABLE_CSS_CLASS` | `wikitable` | Pick the table with this CSS class first |
| `TABLE_INDEX` | `0` | Fallback: 0-based index if CSS class isn't found |
| `TABLE_NAME` | `html_data` | Destination table name |
| `KAFKA_BROKERS` | `localhost:9092` | Comma-separated list of brokers |
| `KAFKA_TOPIC` | `html-table-records` | Topic to produce/consume |
| `KAFKA_GROUP_ID` | `html-pipeline-consumer` | Consumer group ID |
| `DB_DRIVER` | `mysql` | `mysql` or `postgres` |
| `DB_DSN` | **required** | e.g. `user:pass@tcp(host:3306)/db?parseTime=true` |
| `FETCH_TIMEOUT_SEC` | `30` | HTTP timeout in seconds |
| `FETCH_RETRIES` | `3` | Max retry attempts |
| `BATCH_SIZE` | `100` | Rows per Kafka batch / DB insert |
| `BATCH_FLUSH_MS` | `5000` | Consumer flush interval (ms) |

---

## Design decisions

### Idempotent writes
Each Kafka message includes a `_row_hash` — a SHA-256 of the record's JSON. The destination table has a `UNIQUE` constraint on this column. MySQL uses `INSERT IGNORE`; Postgres uses `ON CONFLICT (_row_hash) DO NOTHING`. Re-running the producer against the same page never creates duplicates.

### At-least-once delivery
The consumer calls `FetchMessage` and commits offsets explicitly after each successful DB batch — no auto-commit. If the process crashes mid-batch, Kafka re-delivers those messages and the `_row_hash` deduplication silently handles any rows that were already written.

### Schema carried in messages
Each Kafka message includes the full table schema alongside the row data. This means the consumer can create the destination table on first delivery with no out-of-band coordination or schema registry required.

### Type inference
```
BIGINT → DOUBLE → TIMESTAMP → DATE → VARCHAR(512) → TEXT
```
Every column starts as BIGINT and widens only when a value doesn't fit. Currency symbols (`$`, `€`, `£`, `¥`, `₹`), thousands separators, and `%` suffixes are stripped before numeric parsing. Duplicate column names produced by sanitization are disambiguated automatically (`change`, `change_2`, …).

---

## Project structure

```
cmd/
  producer/   fetch → parse → infer → publish to Kafka
  consumer/   read Kafka → ensure table → batch insert
internal/
  fetcher/    HTTP client with retry and backoff
  parser/     HTML table extractor (colspan, rowspan, CSS class selection)
  schema/     type inference, name sanitization, DDL generation
  kafka/      producer and consumer wrappers
  db/         dynamic DDL, idempotent batch inserts (MySQL + Postgres)
  models/     shared Message / SchemaInfo / ColumnInfo types
config/       environment-variable loader
```

---

## Running tests

```bash
# Unit tests — no network or Docker required
go test ./internal/... -short

# Full integration tests — hits real URLs (requires internet)
go test ./internal/... -v -timeout 120s
```

---

## Known limitations

- **JavaScript-rendered tables** — the fetcher does a plain HTTP GET, so pages that build their tables via JavaScript (Yahoo Finance, Worldometers, etc.) won't work
- **Multi-row headers** — only the first `<th>` row is used as the header; nested or merged header structures are treated as data rows
- **Schema evolution** — `CREATE TABLE IF NOT EXISTS` preserves the original schema across runs; if the source table gains or loses columns, mismatched rows are logged and skipped rather than migrated
- **Non-UTF-8 pages** — charset detection is not implemented; pages served in other encodings may produce garbled column names or values
