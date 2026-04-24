#!/bin/bash

# Configuration
FLOCI_DUCK_URL="http://localhost:3000/execute"
S3_ENDPOINT="http://floci:4566"
S3_REGION="${FLOCI_DUCK_S3_REGION:-us-east-1}"

# 1. Firehose Mode: Test if httpfs is working by querying a public CSV
echo "--- Testing HTTP Query (requires httpfs) ---"
cat <<EOF > payload.json
{
  "sql": "SELECT * FROM 'https://raw.githubusercontent.com/duckdb/duckdb/master/data/csv/floci.csv' LIMIT 3;",
  "s3_endpoint": "$S3_ENDPOINT",
  "s3_region": "$S3_REGION",
  "variables": {}
}
EOF

curl -s -X POST "$FLOCI_DUCK_URL" \
     -H "Content-Type: application/json" \
     -d @payload.json | jq .

# Cleanup
rm payload.json
