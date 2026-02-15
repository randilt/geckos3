# geckos3

A lightweight S3-compatible object storage server that maps buckets to directories and objects to files on the local filesystem. Single binary, zero dependencies, pure Go.

> **Note:** geckos3 is designed for **local development**, **testing**, **CI pipelines**, and **self-hosted single-node setups** where you need an S3-compatible API without the overhead of a full object storage system. It is **not** intended for production workloads that require replication, high availability, or multi-node clustering.

## Quick Start

### Using Docker (Recommended)

```bash
# Pull and run from Docker Hub
docker run -d -p 9000:9000 \
  -e GECKOS3_ACCESS_KEY=mykey \
  -e GECKOS3_SECRET_KEY=mysecret \
  -v ./data:/data \
  randiltharusha/geckos3:latest
```

### Build from Source

```bash
# Build and run
make run

# Or manually
go build -o geckos3
./geckos3
```

Server starts on `http://localhost:9000` with default credentials `geckoadmin`/`geckoadmin`.

## Configuration

All settings can be set via flags or environment variables:

| Flag          | Env Var                | Default      | Description                         |
| ------------- | ---------------------- | ------------ | ----------------------------------- |
| `-data-dir`   | `GECKOS3_DATA_DIR`     | `./data`     | Root directory for bucket storage   |
| `-listen`     | `GECKOS3_LISTEN`       | `:9000`      | HTTP listen address                 |
| `-access-key` | `GECKOS3_ACCESS_KEY`   | `geckoadmin` | AWS access key ID                   |
| `-secret-key` | `GECKOS3_SECRET_KEY`   | `geckoadmin` | AWS secret access key               |
| `-auth`       | `GECKOS3_AUTH_ENABLED` | `true`       | Enable/disable SigV4 authentication |

```bash
# Custom configuration
./geckos3 -data-dir=/mnt/storage -listen=:8080 -access-key=mykey -secret-key=mysecret

# Or with environment variables
export GECKOS3_ACCESS_KEY=mykey
export GECKOS3_SECRET_KEY=mysecret
./geckos3

# Disable auth for local development
./geckos3 -auth=false
```

## Supported S3 Operations

| Operation     | Method   | Path                                           |
| ------------- | -------- | ---------------------------------------------- |
| ListBuckets   | `GET`    | `/`                                            |
| CreateBucket  | `PUT`    | `/{bucket}`                                    |
| DeleteBucket  | `DELETE` | `/{bucket}`                                    |
| HeadBucket    | `HEAD`   | `/{bucket}`                                    |
| ListObjectsV1 | `GET`    | `/{bucket}`                                    |
| ListObjectsV2 | `GET`    | `/{bucket}?list-type=2`                        |
| PutObject     | `PUT`    | `/{bucket}/{key}`                              |
| GetObject     | `GET`    | `/{bucket}/{key}`                              |
| HeadObject    | `HEAD`   | `/{bucket}/{key}`                              |
| DeleteObject  | `DELETE` | `/{bucket}/{key}`                              |
| CopyObject    | `PUT`    | `/{bucket}/{key}` + `x-amz-copy-source` header |
| DeleteObjects | `POST`   | `/{bucket}?delete`                             |

**ListObjectsV1** supports `prefix`, `delimiter`, `max-keys`, and `marker` parameters.

**ListObjectsV2** supports `prefix`, `delimiter`, `max-keys`, `start-after`, and `continuation-token` parameters. When `delimiter` is set, common prefixes are grouped and returned.

**CopyObject** is triggered by setting the `x-amz-copy-source` header (value: `/{source-bucket}/{source-key}`) on a PUT request. Content-Type is preserved from the source.

**GetObject** supports HTTP `Range` requests for partial content retrieval.

**Content-Type** is preserved — the Content-Type sent during PUT is stored and returned on GET/HEAD.

## Usage with AWS CLI

```bash
# Configure
aws configure set aws_access_key_id geckoadmin
aws configure set aws_secret_access_key geckoadmin

# Use with --endpoint-url
aws --endpoint-url http://localhost:9000 s3 mb s3://mybucket
aws --endpoint-url http://localhost:9000 s3 cp file.txt s3://mybucket/file.txt
aws --endpoint-url http://localhost:9000 s3 ls s3://mybucket
aws --endpoint-url http://localhost:9000 s3 cp s3://mybucket/file.txt downloaded.txt
aws --endpoint-url http://localhost:9000 s3 rm s3://mybucket/file.txt
aws --endpoint-url http://localhost:9000 s3 rb s3://mybucket
```

## Usage with boto3

```python
import boto3

s3 = boto3.client(
    "s3",
    endpoint_url="http://localhost:9000",
    aws_access_key_id="geckoadmin",
    aws_secret_access_key="geckoadmin",
    region_name="us-east-1",
)

s3.create_bucket(Bucket="mybucket")
s3.put_object(Bucket="mybucket", Key="hello.txt", Body=b"Hello, World!")
obj = s3.get_object(Bucket="mybucket", Key="hello.txt")
print(obj["Body"].read().decode())
```

## Docker

### Pull from Docker Hub

```bash
# Latest version
docker pull randiltharusha/geckos3:latest

# Specific version
docker pull randiltharusha/geckos3:v0.1.0
```

### Run Container

```bash
# With custom credentials (recommended)
docker run -d -p 9000:9000 \
  -e GECKOS3_ACCESS_KEY=mykey \
  -e GECKOS3_SECRET_KEY=mysecret \
  -v ./data:/data \
  --name geckos3 \
  randiltharusha/geckos3:latest

# With auth disabled (development only)
docker run -d -p 9000:9000 \
  -e GECKOS3_AUTH_ENABLED=false \
  -v ./data:/data \
  --name geckos3 \
  randiltharusha/geckos3:latest
```

### Docker Compose

```bash
docker compose up -d
```

For production, set credentials via environment variables or a `.env` file rather than leaving the defaults in `docker-compose.yml`.

### Build Your Own Image

```bash
make docker-build
```

## Health Check

`GET /health` returns `200 OK` and bypasses authentication. This is used by the Docker health check and is suitable for load balancer probes.

## Bucket Naming Rules

Bucket names must be 3–63 characters, lowercase alphanumeric plus hyphens and dots. No leading/trailing hyphens or dots, no consecutive dots (`..`).

## How It Works

- Buckets are directories under the data dir
- Objects are files within bucket directories
- Nested keys (e.g. `dir/file.txt`) create subdirectories automatically
- Metadata (ETag, Content-Type, timestamps) is stored in `.metadata.json` sidecar files
- Authentication uses AWS Signature Version 4 (header and presigned URL)
- All writes are atomic (temp file + fsync + rename)
- Path traversal is blocked — keys that escape the data directory are rejected

## Logging

Structured JSON logs are written to stdout on every request:

```json
{
  "timestamp": "2026-02-15T12:00:00Z",
  "request_id": "geckos3-1",
  "method": "PUT",
  "uri": "/mybucket/file.txt",
  "status": 200,
  "duration_ms": 3,
  "bytes": 0,
  "client_ip": "127.0.0.1:54321"
}
```

## Make Targets

```
make build           Build the binary
make run             Build and run
make run-dev         Run with auth disabled
make test            Run tests
make bench           Run benchmarks
make clean           Remove build artifacts and data/
make docker-build    Build Docker image
make docker-run      Run in Docker
make docker-stop     Stop Docker container
make install         Install to /usr/local/bin
make fmt             Format code
make lint            Run go vet
```

## Limitations

- No multipart upload
- No versioning, lifecycle policies, or ACLs
- No TLS — use a reverse proxy (nginx, Caddy) for HTTPS
- No rate limiting — use a reverse proxy for rate limiting
- No upload size limit — relies on filesystem quotas
- Single-node only, no replication

## License

MIT
