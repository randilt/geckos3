# geckos3: Ultra-Lightweight S3-Compatible Object Storage

A minimal, production-ready S3-compatible object storage server in pure Go. Exposes a local filesystem through an S3 HTTP API with AWS SDK compatibility.

## Features

✅ **Ultra-lightweight**: < 20MB RAM idle, < 100ms startup  
✅ **S3 API compatible**: Works with AWS SDKs (boto3, aws-sdk-go, aws-sdk-js, etc.)  
✅ **Filesystem-first**: Direct mapping of buckets → directories, objects → files  
✅ **AWS Signature V4**: Full authentication support including presigned URLs  
✅ **Zero dependencies**: Pure Go stdlib only  
✅ **Streaming**: Never loads full files into memory  
✅ **Production-ready**: Atomic writes, proper error handling, structured logging

## Quick Start

### Build

```bash
go build -o geckos3
```

### Run with default settings

```bash
./geckos3
# Server starts on :9000 with data directory ./data
# Default credentials: minioadmin / minioadmin
```

### Run with custom configuration

```bash
./geckos3 \
  -data-dir=/var/lib/s3-data \
  -listen=:8000 \
  -access-key=myaccesskey \
  -secret-key=mysecretkey
```

### Environment Variables

```bash
export S3LITE_DATA_DIR=/var/lib/s3-data
export S3LITE_LISTEN=:8000
export S3LITE_ACCESS_KEY=myaccesskey
export S3LITE_SECRET_KEY=mysecretkey
export S3LITE_AUTH_ENABLED=true

./geckos3
```

## Configuration Options

| Flag          | Environment Variable  | Default      | Description                   |
| ------------- | --------------------- | ------------ | ----------------------------- |
| `-data-dir`   | `S3LITE_DATA_DIR`     | `./data`     | Root directory for buckets    |
| `-listen`     | `S3LITE_LISTEN`       | `:9000`      | HTTP server bind address      |
| `-access-key` | `S3LITE_ACCESS_KEY`   | `minioadmin` | AWS access key                |
| `-secret-key` | `S3LITE_SECRET_KEY`   | `minioadmin` | AWS secret key                |
| `-auth`       | `S3LITE_AUTH_ENABLED` | `true`       | Enable/disable authentication |

## Supported S3 Operations

### Bucket Operations

- ✅ **CreateBucket** (PUT /bucket)
- ✅ **DeleteBucket** (DELETE /bucket)
- ✅ **HeadBucket** (HEAD /bucket)
- ✅ **ListObjectsV2** (GET /bucket?list-type=2)

### Object Operations

- ✅ **PutObject** (PUT /bucket/key)
- ✅ **GetObject** (GET /bucket/key)
- ✅ **HeadObject** (HEAD /bucket/key)
- ✅ **DeleteObject** (DELETE /bucket/key)

### Authentication

- ✅ **AWS Signature Version 4** (Authorization header)
- ✅ **Presigned URLs** (Query parameter auth)

## Usage Examples

### AWS CLI

```bash
# Configure AWS CLI
aws configure set aws_access_key_id minioadmin
aws configure set aws_secret_access_key minioadmin

# Create a bucket
aws --endpoint-url=http://localhost:9000 s3 mb s3://mybucket

# Upload a file
aws --endpoint-url=http://localhost:9000 s3 cp file.txt s3://mybucket/

# List objects
aws --endpoint-url=http://localhost:9000 s3 ls s3://mybucket/

# Download a file
aws --endpoint-url=http://localhost:9000 s3 cp s3://mybucket/file.txt downloaded.txt

# Delete an object
aws --endpoint-url=http://localhost:9000 s3 rm s3://mybucket/file.txt

# Delete a bucket
aws --endpoint-url=http://localhost:9000 s3 rb s3://mybucket
```

### Python (boto3)

```python
import boto3

# Create S3 client
s3 = boto3.client(
    's3',
    endpoint_url='http://localhost:9000',
    aws_access_key_id='minioadmin',
    aws_secret_access_key='minioadmin'
)

# Create bucket
s3.create_bucket(Bucket='mybucket')

# Upload file
with open('file.txt', 'rb') as f:
    s3.put_object(Bucket='mybucket', Key='file.txt', Body=f)

# List objects
response = s3.list_objects_v2(Bucket='mybucket')
for obj in response.get('Contents', []):
    print(obj['Key'])

# Download file
s3.download_file('mybucket', 'file.txt', 'downloaded.txt')

# Delete object
s3.delete_object(Bucket='mybucket', Key='file.txt')

# Delete bucket
s3.delete_bucket(Bucket='mybucket')
```

### Go (aws-sdk-go-v2)

```go
package main

import (
    "context"
    "strings"

    "github.com/aws/aws-sdk-go-v2/aws"
    "github.com/aws/aws-sdk-go-v2/config"
    "github.com/aws/aws-sdk-go-v2/credentials"
    "github.com/aws/aws-sdk-go-v2/service/s3"
)

func main() {
    ctx := context.Background()

    // Create S3 client
    cfg, _ := config.LoadDefaultConfig(ctx,
        config.WithCredentialsProvider(
            credentials.NewStaticCredentialsProvider("minioadmin", "minioadmin", ""),
        ),
    )

    client := s3.NewFromConfig(cfg, func(o *s3.Options) {
        o.BaseEndpoint = aws.String("http://localhost:9000")
        o.UsePathStyle = true
    })

    // Create bucket
    client.CreateBucket(ctx, &s3.CreateBucketInput{
        Bucket: aws.String("mybucket"),
    })

    // Upload object
    client.PutObject(ctx, &s3.PutObjectInput{
        Bucket: aws.String("mybucket"),
        Key:    aws.String("file.txt"),
        Body:   strings.NewReader("Hello, S3!"),
    })

    // List objects
    resp, _ := client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
        Bucket: aws.String("mybucket"),
    })

    for _, obj := range resp.Contents {
        println(*obj.Key)
    }
}
```

### JavaScript (aws-sdk-js)

```javascript
const AWS = require("aws-sdk");

// Configure S3 client
const s3 = new AWS.S3({
  endpoint: "http://localhost:9000",
  accessKeyId: "minioadmin",
  secretAccessKey: "minioadmin",
  s3ForcePathStyle: true,
  signatureVersion: "v4",
});

// Create bucket
await s3.createBucket({ Bucket: "mybucket" }).promise();

// Upload file
await s3
  .putObject({
    Bucket: "mybucket",
    Key: "file.txt",
    Body: "Hello, S3!",
  })
  .promise();

// List objects
const response = await s3.listObjectsV2({ Bucket: "mybucket" }).promise();
response.Contents.forEach((obj) => console.log(obj.Key));

// Download file
const data = await s3
  .getObject({
    Bucket: "mybucket",
    Key: "file.txt",
  })
  .promise();

// Delete object
await s3.deleteObject({ Bucket: "mybucket", Key: "file.txt" }).promise();

// Delete bucket
await s3.deleteBucket({ Bucket: "mybucket" }).promise();
```

## Docker Usage

### Build Docker image

```dockerfile
FROM golang:1.21-alpine AS builder
WORKDIR /build
COPY . .
RUN go build -ldflags="-s -w" -o geckos3

FROM alpine:latest
RUN apk --no-cache add ca-certificates
WORKDIR /app
COPY --from=builder /build/geckos3 .
VOLUME ["/data"]
EXPOSE 9000
CMD ["./geckos3", "-data-dir=/data"]
```

### Run container

```bash
docker build -t geckos3 .
docker run -d -p 9000:9000 -v $(pwd)/data:/data geckos3
```

## Filesystem Layout

```
data/
├── bucket1/
│   ├── file1.txt
│   ├── file1.txt.metadata.json
│   ├── subdir/
│   │   ├── file2.txt
│   │   └── file2.txt.metadata.json
│   └── ...
├── bucket2/
│   └── ...
└── ...
```

- **Buckets** = top-level directories under `data/`
- **Objects** = files at any depth within bucket directories
- **Metadata** = JSON sidecar files with `.metadata.json` extension

### Metadata Format

```json
{
  "size": 1024,
  "lastModified": "2024-01-15T10:30:00Z",
  "etag": "\"d41d8cd98f00b204e9800998ecf8427e\""
}
```

## Logging

All requests are logged to stdout as single-line JSON:

```json
{"timestamp":"2024-01-15T10:30:00Z","method":"PUT","path":"/mybucket/file.txt","status":200,"duration_ms":15,"bytes":1024}
{"timestamp":"2024-01-15T10:30:05Z","method":"GET","path":"/mybucket/file.txt","status":200,"duration_ms":8,"bytes":1024}
```

## Performance Characteristics

- **Startup time**: < 100ms
- **Idle memory**: < 20MB RSS
- **Streaming**: All file I/O uses `io.Copy` - no memory buffering
- **Concurrent requests**: Handled efficiently via Go's goroutine scheduler
- **File operations**: Direct filesystem calls with atomic renames for writes

## Limitations

- **Single node only**: No clustering or replication
- **No versioning**: Single version per object
- **No lifecycle policies**: Manual management required
- **No server-side encryption**: Store on encrypted filesystem if needed
- **No ACLs**: All-or-nothing bucket access
- **Path-style only**: Virtual-host style URLs not supported
- **No multipart uploads**: Upload size limited by available memory/disk

## Architecture

### Core Components

1. **HTTP Server** (`main.go`): Request routing and initialization
2. **S3 Handler** (`handler.go`): S3 protocol implementation
3. **Storage Layer** (`storage.go`): Filesystem operations
4. **Authentication** (`auth.go`): AWS Signature V4 verification
5. **Logging** (`logging.go`): Request/response logging

### Design Principles

- **Simplicity first**: Minimal code, clear logic
- **Streaming everywhere**: Never buffer entire files
- **Filesystem transparency**: Human-readable structure
- **Atomic operations**: Temp file + rename pattern
- **Zero dependencies**: Pure Go stdlib only

## Development

### Run tests

```bash
go test ./...
```

### Build statically

```bash
CGO_ENABLED=0 go build -ldflags="-s -w" -o geckos3
```

### Memory profiling

```bash
# Start with profiling
go run . &
PID=$!

# Monitor memory
watch -n 1 "ps aux | grep geckos3 | grep -v grep"

# Kill when done
kill $PID
```

## Use Cases

- **Local development**: S3 testing without AWS costs
- **CI/CD pipelines**: Artifact storage in test environments
- **Edge deployments**: Small-scale object storage
- **Backup destinations**: Simple, inspectable backup target
- **Self-hosted apps**: Embedded object storage for SaaS products
- **IoT gateways**: Lightweight storage for edge devices

## License

MIT License - see LICENSE file for details

## Contributing

Contributions welcome! Please ensure:

- Code uses only Go stdlib
- Memory usage remains < 20MB idle
- Startup time remains < 100ms
- All S3 API changes maintain AWS SDK compatibility
