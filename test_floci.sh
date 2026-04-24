#!/bin/bash

# Configuration
FLOCI_DUCK_URL="http://localhost:3000/execute"
S3_ENDPOINT="http://floci:4566"
S3_REGION="${FLOCI_DUCK_S3_REGION:-us-east-1}"

# 1. Firehose Mode: Direct execution (e.g., creating a table and inserting data)
echo "--- Testing Firehose Mode (Direct SQL) ---"
cat <<EOF > payload.json
{
  "sql": "CREATE TABLE test_data AS SELECT * FROM (VALUES (1, 'Alice'), (2, 'Bob')) t(id, name);",
  "s3_endpoint": "$S3_ENDPOINT",
  "s3_region": "$S3_REGION",
  "variables": {}
}
EOF

curl -s -X POST "$FLOCI_DUCK_URL" \
     -H "Content-Type: application/json" \
     -d @payload.json | jq .

# 2. Athena Mode: Querying with output redirection to S3
echo -e "\n--- Testing Athena Mode (Query with S3 Output) ---"
cat <<EOF > payload.json
{
  "sql": "SELECT * FROM range(10) t(val)",
  "s3_endpoint": "$S3_ENDPOINT",
  "s3_region": "$S3_REGION",
  "output_s3_path": "s3://test-bucket/results/query_$(date +%s).csv",
  "variables": {}
}
EOF

# Note: DuckDB requires the bucket to exist. 
# In a real local setup, you'd ensure the bucket exists in Floci first.
curl -s -X POST "$FLOCI_DUCK_URL" \
     -H "Content-Type: application/json" \
     -d @payload.json | jq .

# Cleanup
rm payload.json
