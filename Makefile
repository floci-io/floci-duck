# Makefile for floci-duck development

.PHONY: dev-infra stop-infra run test-athena test-firehose clean

# Starts the local Floci infrastructure
dev-infra:
	docker-compose up -d floci
	./init_resources.sh

# Stops the local Floci infrastructure
stop-infra:
	docker-compose stop floci

# Runs the sidecar locally (Native performance, no Docker overhead)
# Note: Use localhost:9000 in your tests when running natively
run:
	FLOCI_DUCK_LOG=info cargo run

# Runs the sidecar locally with automatic re-compilation (Requires cargo-watch)
watch:
	cargo watch -x run

# Quick integration test (Native mode)
# Overrides S3_ENDPOINT to localhost for native execution
test-native:
	S3_ENDPOINT="http://localhost:9000" ./test_floci.sh

# Full Docker-based development loop
# (Uses the Dockerfile but without the release/static constraints if preferred)
docker-dev:
	docker-compose up --build
