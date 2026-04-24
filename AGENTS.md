# Project Memory: floci-duck Sidecar

## Project Vision
`floci-duck` is a high-performance Rust sidecar designed to provide local AWS Athena and Firehose emulation. It wraps the DuckDB engine to execute SQL queries and handle S3 data transformations locally, primarily for testing and development environments within the `floci` ecosystem.

## Core Architecture
- **Web Layer:** Axum/Tokio HTTP server handling POST requests.
- **Engine:** DuckDB (using the `bundled` feature for a zero-dependency static build).
- **Isolation:** Each request uses a unique `Connection::open_in_memory()` to ensure strict query isolation.
- **S3 Compatibility:** Uses the `httpfs` extension to interact with S3-compatible storage (like Floci).
- **Concurrency:** DuckDB queries run in `tokio::task::spawn_blocking` to avoid blocking the async runtime.

## API Interface
- **Endpoint:** `POST /execute`
- **Body limit:** 1 MB
- **Headers:**
  - `X-Correlation-ID` (optional): propagated through all logs for the request; auto-generated (UUID v4) if absent.
- **Payload Schema:**
  ```json
  {
    "sql": "string",
    "s3_endpoint": "string",
    "s3_region": "string (optional)",
    "s3_access_key": "string (optional)",
    "s3_secret_key": "string (optional)",
    "s3_use_ssl": "bool (optional)",
    "s3_url_style": "string (optional)",
    "output_s3_path": "string (optional)",
    "setup_sql": "string (optional)",
    "variables": { "key": "value" }
  }
  ```
- **Athena Mode:** Triggered by `output_s3_path`. Automatically wraps the SQL in a `COPY (...) TO 'path' (FORMAT CSV, HEADER);` statement.
- **Firehose Mode:** Triggered when `output_s3_path` is absent. Executes the SQL exactly as provided.
- **Variable substitution:** `{{key}}` placeholders in `sql` and `setup_sql` are replaced with values from the `variables` map before execution.

## Build & Deployment
- **Targets:** `x86_64-unknown-linux-gnu` (amd64) and `aarch64-unknown-linux-gnu` (arm64), built inside `rust:slim-bookworm` to match the glibc version of the runtime image.
- **Base Image:** `gcr.io/distroless/cc-debian12` (minimal, non-root, includes glibc).
- **CI:** `semver.yml` runs semantic-release on push to main, creating a version tag. `release.yml` triggers on that tag to build binaries and push a multi-arch Docker image.

## Local Development Stack (`docker-compose.yml`)
1. **`floci`**: Acts as the AWS S3 emulator (port 4566).
2. **`floci-duck`**: The sidecar service (port 3000).

## Environment Variables
- `FLOCI_DUCK_PORT`: HTTP server port (default: 3000).
- `FLOCI_DUCK_EXT_DIR`: Path to a local directory for DuckDB extensions (for offline use).
- `FLOCI_DUCK_LOG`: Logging level (e.g., `info`, `debug`).
- `FLOCI_DUCK_S3_REGION`: Default S3 region (default: `us-east-1`).
- `FLOCI_DUCK_S3_ACCESS_KEY`: Default S3 access key (default: `flociadmin`).
- `FLOCI_DUCK_S3_SECRET_KEY`: Default S3 secret key (default: `flociadmin`).
- `FLOCI_DUCK_S3_USE_SSL`: Default S3 SSL usage (default: derived from endpoint scheme).
- `FLOCI_DUCK_S3_URL_STYLE`: Default S3 URL style (default: `path`).

## Testing Strategy
- Unit tests in `src/executor.rs` verify SQL wrapping, variable substitution, and SQL escaping.
- `local/test_floci.sh` provides a CLI-based integration test for both Firehose and Athena modes.
