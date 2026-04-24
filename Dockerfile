# Stage 1: Builder
FROM rust:slim-bookworm AS builder

RUN apt-get update && apt-get install -y \
    build-essential \
    cmake \
    libssl-dev \
    libcurl4-openssl-dev \
    pkg-config

WORKDIR /usr/src/floci-duck

# Copy project files
COPY Cargo.toml Cargo.lock ./
COPY src ./src

# Build the application
RUN cargo build --release

# Strip the binary
RUN strip target/release/floci-duck

# Stage 2: Final
FROM gcr.io/distroless/cc-debian12

# Copy the binary
COPY --from=builder /usr/src/floci-duck/target/release/floci-duck /floci-duck

EXPOSE 3000
ENTRYPOINT ["/floci-duck"]
