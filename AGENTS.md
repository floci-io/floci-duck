# Project Memory: floci-duck Sidecar

## Project Vision
`floci-duck` is a high-performance, statically-linked Rust sidecar designed to provide local AWS Athena and Firehose emulation. It wraps the DuckDB engine to execute SQL queries and handle S3 data transformations locally, primarily for testing and development environments within the `floci` ecosystem.

## Core Architecture
- **Web Layer:** Axum/Tokio HTTP server handling POST requests.
- **Engine:** DuckDB (using the `bundled` feature for a zero-dependency static build).
- **Isolation:** Each request uses a unique `Connection::open_in_memory()` to ensure strict query isolation.
- **S3 Compatibility:** Uses the `httpfs` extension to interact with S3-compatible storage (like Floci).

## API Interface
- **Endpoint:** `POST /execute`
- **Payload Schema:**
  ```json
  {
    "sql": "string",
    "s3_endpoint": "string",
    "s3_region": "string",
    "output_s3_path": "string (optional)",
    "variables": { "key": "value" }
  }
  ```
- **Athena Mode:** Triggered by `output_s3_path`. Automatically wraps the SQL in a `COPY (...) TO 'path' (FORMAT CSV, HEADER);` statement.
- **Firehose Mode:** Triggered when `output_s3_path` is absent. Executes the SQL exactly as provided.

## Build & Deployment
- **Target:** `x86_64-unknown-linux-musl` for a fully static binary.
- **Docker:** Multi-stage build using `messense/rust-musl-cross`.
- **Base Image:** `scratch` to achieve a total image size < 50MB.

## Local Development Stack (`docker-compose.yml`)
1. **`floci`**: Acts as the AWS S3 emulator.
2. **`floci-duck`**: The sidecar service.

## Environment Variables
- `FLOCI_DUCK_PORT`: HTTP server port (default: 3000).
- `FLOCI_DUCK_EXT_DIR`: Path to a local directory for DuckDB extensions (for offline use).
- `FLOCI_DUCK_LOG`: Logging level (e.g., `info`, `debug`).
- `FLOCI_DUCK_S3_REGION`: Default S3 region (default: `us-east-1`).
- `FLOCI_DUCK_S3_ACCESS_KEY`: Default S3 access key (default: `flociadmin`).
- `FLOCI_DUCK_S3_SECRET_KEY`: Default S3 secret key (default: `flociadmin`).
- `FLOCI_DUCK_S3_USE_SSL`: Default S3 SSL usage (default: `false`).
- `FLOCI_DUCK_S3_URL_STYLE`: Default S3 URL style (default: `path`).

## Testing Strategy
- Unit tests in `src/main.rs` verify SQL wrapping logic.
- `test_floci.sh` provides a CLI-based integration test for both Firehose and Athena modes.
