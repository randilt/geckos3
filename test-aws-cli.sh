#!/bin/bash
set -e

# geckos3 Test Script
# This script demonstrates basic S3 operations using AWS CLI

ENDPOINT="http://localhost:9000"
BUCKET="testbucket"
ACCESS_KEY="geckoadmin"
SECRET_KEY="geckoadmin"

echo "==> Configuring AWS CLI..."
export AWS_ACCESS_KEY_ID=$ACCESS_KEY
export AWS_SECRET_ACCESS_KEY=$SECRET_KEY

echo ""
echo "==> Creating bucket: $BUCKET"
aws --endpoint-url=$ENDPOINT s3 mb s3://$BUCKET

echo ""
echo "==> Creating test file..."
echo "Hello, geckos3!" > /tmp/test-file.txt

echo ""
echo "==> Uploading file to bucket..."
aws --endpoint-url=$ENDPOINT s3 cp /tmp/test-file.txt s3://$BUCKET/

echo ""
echo "==> Listing objects in bucket..."
aws --endpoint-url=$ENDPOINT s3 ls s3://$BUCKET/

echo ""
echo "==> Creating directory structure..."
echo "File 1" > /tmp/file1.txt
echo "File 2" > /tmp/file2.txt
aws --endpoint-url=$ENDPOINT s3 cp /tmp/file1.txt s3://$BUCKET/dir1/file1.txt
aws --endpoint-url=$ENDPOINT s3 cp /tmp/file2.txt s3://$BUCKET/dir1/file2.txt

echo ""
echo "==> Listing all objects..."
aws --endpoint-url=$ENDPOINT s3 ls s3://$BUCKET/ --recursive

echo ""
echo "==> Downloading file..."
aws --endpoint-url=$ENDPOINT s3 cp s3://$BUCKET/test-file.txt /tmp/downloaded.txt
cat /tmp/downloaded.txt

echo ""
echo "==> Getting object metadata..."
aws --endpoint-url=$ENDPOINT s3api head-object --bucket $BUCKET --key test-file.txt

echo ""
echo "==> Deleting objects..."
aws --endpoint-url=$ENDPOINT s3 rm s3://$BUCKET/test-file.txt
aws --endpoint-url=$ENDPOINT s3 rm s3://$BUCKET/dir1/file1.txt
aws --endpoint-url=$ENDPOINT s3 rm s3://$BUCKET/dir1/file2.txt

echo ""
echo "==> Deleting bucket..."
aws --endpoint-url=$ENDPOINT s3 rb s3://$BUCKET

echo ""
echo "==> Cleaning up temp files..."
rm -f /tmp/test-file.txt /tmp/file1.txt /tmp/file2.txt /tmp/downloaded.txt

echo ""
echo "✅ All tests passed!"