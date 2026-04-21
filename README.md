# Floci Duck

[![Release](https://github.com/floci-io/floci-duck/actions/workflows/release.yml/badge.svg)](https://github.com/floci-io/floci-duck/actions/workflows/release.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

A lightweight DuckDB-powered executor sidecar for [Floci](https://github.com/floci-io/floci). It provides an HTTP interface to execute SQL queries using DuckDB, with built-in support for S3-compatible storage via the `httpfs` extension.

## Features

- **DuckDB Integration**: Leverages the high-performance DuckDB engine.
- **S3 Support**: Built-in integration with S3-compatible storage.
- **Firehose Mode**: Direct SQL execution for DDL/DML operations.
- **Athena Mode**: Automated query result export to S3 in CSV format (similar to AWS Athena).
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

## API Reference

### POST `/execute`

Executes a SQL query.

#### Request Body

> **Note**: S3 credentials (`s3_access_key`, `s3_secret_key`) are optional in the request if you have set the corresponding `FLOCI_DUCK_S3_*` environment variables.

| Field | Type | Description |
| :--- | :--- | :--- |
| `sql` | String | The SQL query to execute. |
| `s3_endpoint` | String | S3-compatible endpoint (e.g., `http://floci:4566`). |
| `s3_region` | String (Optional) | S3 region (e.g., `us-east-1`). |
| `s3_access_key` | String (Optional) | S3 access key ID. |
| `s3_secret_key` | String (Optional) | S3 secret access key. |
| `s3_use_ssl` | Boolean (Optional) | Use SSL (HTTPS). Auto-detected if endpoint starts with `https://`. |
| `s3_url_style` | String (Optional) | S3 URL style (`path` or `vhost`). Default: `path`. |
| `output_s3_path` | String (Optional) | If provided, enables **Athena Mode**. The query results will be exported to this path. |
| `variables` | Map (Optional) | Key-value pairs for query variables (placeholder support). |

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

## Configuration

The following environment variables can be used to configure the executor:

| Variable | Default | Description |
| :--- | :--- | :--- |
| `FLOCI_DUCK_PORT` | `3000` | The port the server will listen on. |
| `FLOCI_DUCK_LOG` | `info` | Log level (`error`, `warn`, `info`, `debug`, `trace`). |
| `FLOCI_DUCK_S3_REGION` | `us-east-1` | Default S3 region. |
| `FLOCI_DUCK_S3_ACCESS_KEY` | `flociadmin` | Default S3 access key ID. |
| `FLOCI_DUCK_S3_SECRET_KEY` | `flociadmin` | Default S3 secret access key. |
| `FLOCI_DUCK_S3_USE_SSL` | `false` | Default SSL usage. |
| `FLOCI_DUCK_S3_URL_STYLE` | `path` | Default S3 URL style (`path` or `vhost`). |

## Development

### Testing

You can use the provided test scripts to verify the executor:

- `test_floci.sh`: General integration tests.
- `test_http.sh`: Basic HTTP endpoint tests.

```bash
./test_floci.sh
```

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details (or MIT if not present).

## Related Projects

- [Floci](https://github.com/floci-io/floci): The main Floci project.
