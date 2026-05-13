# Floci Duck

[![Release](https://github.com/floci-io/floci-duck/actions/workflows/release.yml/badge.svg)](https://github.com/floci-io/floci-duck/actions/workflows/release.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

A lightweight DuckDB-powered executor sidecar for [Floci](https://github.com/floci-io/floci). It provides an HTTP interface to execute SQL queries using DuckDB, with built-in support for S3-compatible storage via the `httpfs` extension.

## Features

- **DuckDB Integration**: Leverages the high-performance DuckDB engine.
- **S3 Support**: Built-in integration with S3-compatible storage.
- **Firehose Mode**: Direct SQL execution for DDL/DML operations.
- **Athena Mode**: Automated query result export to S3 in CSV format (similar to AWS Athena).
- **S3 Select Mode**: Execute a query and return rows as JSON via `/query`.
- **Parquet Support**: Read and write Parquet files directly from/to S3.
- **Correlation ID Tracing**: Every request is tagged with a correlation ID propagated through all log lines.
- **Lightweight**: Written in Rust for minimal overhead and high performance.

## Getting Started

### Prerequisites

- [Docker](https://www.docker.com/) and [Docker Compose](https://docs.docker.com/compose/)
- Alternatively, [Rust](https://www.rust-lang.org/) for local development.

### Running with Docker Compose

The easiest way to get started is using the provided `docker-compose.yml`, which spins up both Floci and Floci Duck.

```bash
docker-compose up --build
```

The executor will be available at `http://localhost:3000`.

### Local Build and Run

```bash
cargo build --release
./target/release/floci-duck
```

### Startup behaviour

On startup, floci-duck runs a **preflight check** that installs the `httpfs` DuckDB extension to local disk. This ensures every subsequent request can load the extension instantly without a network download.

```
INFO floci_duck::executor: Preflight: installing httpfs extension...
INFO floci_duck::executor: Preflight: httpfs installed successfully
INFO floci_duck: Starting floci-duck sidecar on 0.0.0.0:3000
```

If the preflight fails (e.g. no network on first boot), a warning is logged and the extension is installed on the first request that needs it.

---

## API Reference

### GET `/health`

Returns `200 OK` with body `OK`. Used for liveness checks.

---

### POST `/query`

Executes a SQL query and returns the result rows as a JSON array. Useful for reading data from S3 (CSV, Parquet, JSON) or running analytical queries inline.

#### Request Body

> **Note**: S3 credentials are optional if the corresponding `FLOCI_DUCK_S3_*` environment variables are set.

| Field | Type | Description |
| :--- | :--- | :--- |
| `sql` | String | The SQL query to execute. |
| `s3_endpoint` | String | S3-compatible endpoint (e.g., `http://floci:4566`). |
| `s3_region` | String (Optional) | S3 region. Defaults to `FLOCI_DUCK_S3_REGION` or `us-east-1`. |
| `s3_access_key` | String (Optional) | S3 access key ID. |
| `s3_secret_key` | String (Optional) | S3 secret access key. |
| `s3_use_ssl` | Boolean (Optional) | Use SSL. Auto-detected from the endpoint scheme if omitted. |
| `s3_url_style` | String (Optional) | `path` or `vhost`. Default: `path`. |
| `setup_sql` | String (Optional) | SQL executed before the main query — use it to create views, temp tables, or load extensions. |

#### Response Body

```json
{
  "status": "success",
  "rows": [
    { "id": 1, "name": "Alice", "amount": 99.5 },
    { "id": 2, "name": "Bob",   "amount": 150.0 }
  ]
}
```

On error, `status` is `"error"` and `message` contains the details. The `rows` field is omitted on error.

#### Example: Read a Parquet file from S3

```json
{
  "sql": "SELECT * FROM 's3://my-bucket/data/sales.parquet' WHERE amount > 100",
  "s3_endpoint": "http://floci:4566"
}
```

#### Example: Query with `setup_sql`

Use `setup_sql` to define a view or register a data source before the main query runs. Both statements execute within the same DuckDB session.

```json
{
  "sql": "SELECT region, SUM(amount) AS total FROM sales GROUP BY region",
  "s3_endpoint": "http://floci:4566",
  "setup_sql": "CREATE VIEW sales AS SELECT * FROM 's3://my-bucket/data/sales.parquet';"
}
```

---

### POST `/execute`

Executes a SQL statement with no row output (fire-and-forget). Supports two modes:

- **Firehose mode** — runs any SQL directly (DDL, DML, COPY, etc.)
- **Athena mode** — wraps the SQL in a `COPY … TO … FORMAT CSV` and writes results to S3

#### Request Body

> **Note**: S3 credentials are optional if the corresponding `FLOCI_DUCK_S3_*` environment variables are set.

| Field | Type | Description |
| :--- | :--- | :--- |
| `sql` | String | The SQL statement to execute. |
| `s3_endpoint` | String | S3-compatible endpoint (e.g., `http://floci:4566`). |
| `s3_region` | String (Optional) | S3 region. Defaults to `FLOCI_DUCK_S3_REGION` or `us-east-1`. |
| `s3_access_key` | String (Optional) | S3 access key ID. |
| `s3_secret_key` | String (Optional) | S3 secret access key. |
| `s3_use_ssl` | Boolean (Optional) | Use SSL. Auto-detected from the endpoint scheme if omitted. |
| `s3_url_style` | String (Optional) | `path` or `vhost`. Default: `path`. |
| `output_s3_path` | String (Optional) | If provided, enables **Athena Mode** — results are exported to this S3 path as CSV. |
| `variables` | Map (Optional) | Key-value pairs substituted into the SQL as `{{key}}` placeholders. |

#### Response Body

```json
{ "status": "success" }
```

On error, `status` is `"error"` and `message` contains the details.

#### Example: Firehose Mode (Direct SQL)

```json
{
  "sql": "CREATE TABLE users AS SELECT * FROM read_csv_auto('s3://bucket/data.csv');",
  "s3_endpoint": "http://floci:4566"
}
```

#### Example: Athena Mode (Query with S3 Output)

```json
{
  "sql": "SELECT name, count(*) FROM users GROUP BY 1",
  "s3_endpoint": "http://floci:4566",
  "s3_region": "us-east-1",
  "output_s3_path": "s3://results-bucket/report.csv"
}
```

#### Example: Variable substitution

```json
{
  "sql": "SELECT * FROM 's3://my-bucket/{{env}}/data.parquet'",
  "s3_endpoint": "http://floci:4566",
  "variables": { "env": "production" }
}
```

---

## Configuration

| Variable | Default | Description |
| :--- | :--- | :--- |
| `FLOCI_DUCK_PORT` | `3000` | Port the server listens on. |
| `FLOCI_DUCK_LOG` | `info` | Log level (`error`, `warn`, `info`, `debug`, `trace`). |
| `FLOCI_DUCK_EXT_DIR` | _(DuckDB default)_ | Override the DuckDB extension directory (useful in Docker to persist extensions across restarts). |
| `FLOCI_DUCK_S3_REGION` | `us-east-1` | Default S3 region. |
| `FLOCI_DUCK_S3_ACCESS_KEY` | `flociadmin` | Default S3 access key ID. |
| `FLOCI_DUCK_S3_SECRET_KEY` | `flociadmin` | Default S3 secret access key. |
| `FLOCI_DUCK_S3_USE_SSL` | auto | Default SSL usage. Auto-detected from the endpoint scheme if not set. |
| `FLOCI_DUCK_S3_URL_STYLE` | `path` | Default S3 URL style (`path` or `vhost`). |

---

## Observability

### Correlation ID

Every request is assigned a **correlation ID** — either taken from the incoming `x-correlation-id` header or auto-generated as a UUID v4. The ID is propagated through every log line produced by that request, including logs emitted deep inside the executor.

#### Log format

The correlation ID appears as a bare value inside the span context — no `key=` prefix:

```
INFO execute{0232f4ad-4ea6-4b24-99e9-4f478998b848}: floci_duck::handlers: Received execute request
INFO execute{0232f4ad-4ea6-4b24-99e9-4f478998b848}: floci_duck::executor: Configuring S3: endpoint=floci:4566, region=us-east-1
INFO execute{0232f4ad-4ea6-4b24-99e9-4f478998b848}: floci_duck::executor: Firehose mode detected. Running raw SQL.
INFO execute{0232f4ad-4ea6-4b24-99e9-4f478998b848}: floci_duck::executor: Executing final SQL: SELECT ...
INFO execute{0232f4ad-4ea6-4b24-99e9-4f478998b848}: floci_duck::handlers: Query executed successfully

INFO query{3e4adc71-366f-4cf7-8ce9-41d441d7e755}: floci_duck::handlers: Received query request
INFO query{3e4adc71-366f-4cf7-8ce9-41d441d7e755}: floci_duck::executor: Configuring S3: endpoint=floci:4566, region=us-east-1
INFO query{3e4adc71-366f-4cf7-8ce9-41d441d7e755}: floci_duck::executor: Executing query SQL: SELECT * FROM ...
INFO query{3e4adc71-366f-4cf7-8ce9-41d441d7e755}: floci_duck::executor: Query returned 4 rows
INFO query{3e4adc71-366f-4cf7-8ce9-41d441d7e755}: floci_duck::handlers: Query returned 4 rows
```

#### Passing a correlation ID from the client

```bash
curl -X POST http://localhost:3000/query \
  -H "Content-Type: application/json" \
  -H "x-correlation-id: my-trace-id-123" \
  -d '{ "sql": "SELECT 1", "s3_endpoint": "http://floci:4566" }'
```

If the header is omitted, a UUID v4 is generated automatically.

---

## Development

### Testing

`duck-test` is the integration test CLI for floci-duck. It covers all endpoints and scenarios in named suites.

#### Prerequisites

- A running floci-duck server (`make run` or `docker-compose up`)
- `jq` and `curl` (always required)
- `aws` CLI (required for `init`, `parquet`, and `validate` suites)

#### Quick start

```bash
# Bring up infrastructure and create S3 resources
make dev-infra

# Run all test suites
./duck-test all

# Run specific suites
./duck-test health query
./duck-test parquet --bucket my-bucket

# Verbose output (prints full JSON responses)
./duck-test all -v
```

#### Suites

| Suite | What it tests |
| :--- | :--- |
| `init` | Creates the S3 bucket and lists resources |
| `health` | Server liveness (`GET /health`) |
| `query` | `/query` endpoint — basic SELECT, NULLs, numeric types, `setup_sql`, correlation ID, error handling |
| `execute` | `/execute` endpoint — firehose mode, athena mode (CSV → S3), variable substitution |
| `parquet` | Full S3 round-trip: write Parquet, SELECT *, filter, aggregate, DESCRIBE schema |
| `http` | `httpfs` extension loads and S3 settings are applied |
| `validate` | Downloads the latest result file from S3 and prints it |
| `all` | Runs every suite in order |

#### Options

```
./duck-test [OPTIONS] <SUITE> [SUITE...]

  --url URL              floci-duck server URL        [default: http://localhost:3000]
  --s3-endpoint URL      S3 endpoint (server-facing)  [default: http://floci:4566]
  --s3-endpoint-cli URL  S3 endpoint (aws CLI / host) [default: http://localhost:4566]
  --s3-region REGION     S3 region                    [default: us-east-1]
  --s3-access-key KEY    S3 access key                [default: flociadmin]
  --s3-secret-key KEY    S3 secret key                [default: flociadmin]
  --bucket BUCKET        S3 bucket name               [default: test-bucket]
  -v, --verbose          Print full response bodies
  -h, --help             Show help
```

All options can also be set via environment variables (`FLOCI_DUCK_URL`, `FLOCI_DUCK_S3_ENDPOINT`, etc.).

#### Example output

```
duck-test  →  http://localhost:3000  |  S3: http://floci:4566

══ health ══
  [PASS] /health → 200 OK

══ parquet  (S3 round-trip) ══
  [PASS] write parquet to S3
  [PASS] SELECT * from parquet
        row count: 4 (expected 4)
  [PASS] parquet filter WHERE amount > 100
  [PASS] parquet aggregate SUM/COUNT
        total=4 (expected 4), revenue=492.25 (expected 492.25)
  [PASS] parquet DESCRIBE schema

══════════════════════════════
  PASS: 6     FAIL: 0     SKIP: 0
══════════════════════════════
```

---

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details (or MIT if not present).

## Related Projects

- [Floci](https://github.com/floci-io/floci): The main Floci project.
