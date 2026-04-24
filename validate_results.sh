#!/bin/bash

# Configuration
S3_ENDPOINT="http://localhost:4566"
AWS_ACCESS_KEY_ID="flociadmin"
AWS_SECRET_ACCESS_KEY="flociadmin"
BUCKET_NAME="test-bucket"
LOCAL_FILE="results_validation.csv"

echo "--- Validating floci-duck Query Results ---"

# Check if aws-cli is installed
if ! command -v aws &> /dev/null
then
    echo "Error: aws-cli is not installed."
    exit 1
fi

# Define the alias for the local S3 call
aws_s3() {
    AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID \
    AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY \
    aws --endpoint-url "$S3_ENDPOINT" s3 "$@"
}

# 1. List results to find the latest file
echo "Searching for results in s3://$BUCKET_NAME/results/..."
LATEST_FILE=$(aws_s3 ls "s3://$BUCKET_NAME/results/" | sort | tail -n 1 | awk '{print $4}')

if [ -z "$LATEST_FILE" ]; then
    echo "No result files found in s3://$BUCKET_NAME/results/"
    exit 1
fi

echo "Downloading latest result: $LATEST_FILE..."

# 2. Download the file
aws_s3 cp "s3://$BUCKET_NAME/results/$LATEST_FILE" "$LOCAL_FILE"

# 3. Display the contents
if [ -f "$LOCAL_FILE" ]; then
    echo -e "\n--- CSV Content ($LATEST_FILE) ---"
    cat "$LOCAL_FILE"
    echo -e "\n--- End of File ---"
    
    # Optional: cleanup
    # rm "$LOCAL_FILE"
else
    echo "Failed to download the file."
    exit 1
fi
