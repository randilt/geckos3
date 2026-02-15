#!/usr/bin/env python3
"""
geckos3 Test Script (Python/boto3)
Demonstrates basic S3 operations using the boto3 library
"""

import boto3
from botocore.client import Config
import sys

# Configuration
ENDPOINT = 'http://localhost:9000'
ACCESS_KEY = 'geckoadmin'
SECRET_KEY = 'geckoadmin'
BUCKET = 'testbucket'

def main():
    # Create S3 client
    print("==> Creating S3 client...")
    s3 = boto3.client(
        's3',
        endpoint_url=ENDPOINT,
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
        config=Config(signature_version='s3v4'),
        region_name='us-east-1'
    )

    try:
        # Create bucket
        print(f"\n==> Creating bucket: {BUCKET}")
        s3.create_bucket(Bucket=BUCKET)
        print("✓ Bucket created")

        # Upload file
        print("\n==> Uploading file...")
        content = b"Hello, geckos3 from Python!"
        s3.put_object(Bucket=BUCKET, Key='test-file.txt', Body=content)
        print("✓ File uploaded")

        # List objects
        print("\n==> Listing objects...")
        response = s3.list_objects_v2(Bucket=BUCKET)
        for obj in response.get('Contents', []):
            print(f"  - {obj['Key']} ({obj['Size']} bytes)")

        # Upload files with directory structure
        print("\n==> Creating directory structure...")
        s3.put_object(Bucket=BUCKET, Key='dir1/file1.txt', Body=b'File 1')
        s3.put_object(Bucket=BUCKET, Key='dir1/file2.txt', Body=b'File 2')
        s3.put_object(Bucket=BUCKET, Key='dir2/file3.txt', Body=b'File 3')
        print("✓ Files uploaded")

        # List all objects
        print("\n==> Listing all objects...")
        response = s3.list_objects_v2(Bucket=BUCKET)
        for obj in response.get('Contents', []):
            print(f"  - {obj['Key']} ({obj['Size']} bytes, ETag: {obj['ETag']})")

        # Get object
        print("\n==> Downloading file...")
        response = s3.get_object(Bucket=BUCKET, Key='test-file.txt')
        downloaded_content = response['Body'].read()
        print(f"✓ Content: {downloaded_content.decode('utf-8')}")

        # Head object
        print("\n==> Getting object metadata...")
        response = s3.head_object(Bucket=BUCKET, Key='test-file.txt')
        print(f"  Size: {response['ContentLength']} bytes")
        print(f"  ETag: {response['ETag']}")
        print(f"  Last Modified: {response['LastModified']}")

        # List with prefix
        print("\n==> Listing objects with prefix 'dir1/'...")
        response = s3.list_objects_v2(Bucket=BUCKET, Prefix='dir1/')
        for obj in response.get('Contents', []):
            print(f"  - {obj['Key']}")

        # Delete objects
        print("\n==> Deleting objects...")
        objects_to_delete = [
            'test-file.txt',
            'dir1/file1.txt',
            'dir1/file2.txt',
            'dir2/file3.txt'
        ]
        for key in objects_to_delete:
            s3.delete_object(Bucket=BUCKET, Key=key)
        print("✓ Objects deleted")

        # Delete bucket
        print(f"\n==> Deleting bucket: {BUCKET}")
        s3.delete_bucket(Bucket=BUCKET)
        print("✓ Bucket deleted")

        print("\n✅ All tests passed!")

    except Exception as e:
        print(f"\n❌ Error: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == '__main__':
    main()