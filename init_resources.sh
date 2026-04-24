#!/bin/bash

# Configuration
S3_ENDPOINT="http://floci:4566"
AWS_ACCESS_KEY_ID="test"
AWS_SECRET_ACCESS_KEY="test"
AWS_REGION="us-east-1"

echo "--- Initializing floci local resources ---"

# Check if aws-cli is installed
if ! command -v aws &> /dev/null
then
    echo "Error: aws-cli is not installed. Please install it to use this script."
    exit 1
fi

# Define the alias for the local S3 call
aws_s3() {
    AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID \
    AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY \
    aws --endpoint-url "$S3_ENDPOINT" s3 "$@"
}

# 1. Create the bucket required for testing
BUCKET_NAME="test-bucket"

echo "Creating bucket: $BUCKET_NAME..."
aws_s3 mb "s3://$BUCKET_NAME" 2>/dev/null || echo "Bucket $BUCKET_NAME already exists."

# 2. List all buckets to confirm
echo -e "\n--- Current S3 Buckets ---"
aws_s3 ls

echo -e "\nResources initialized successfully."
