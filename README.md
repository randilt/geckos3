# 🦎 GeckoS3

[![GitHub Release](https://img.shields.io/github/v/release/randilt/geckos3)](https://github.com/randilt/geckos3/releases)
[![Docker Pulls](https://img.shields.io/docker/pulls/randiltharusha/geckos3)](https://hub.docker.com/r/randiltharusha/geckos3)
[![Go Version](https://img.shields.io/github/go-mod/go-version/randilt/geckos3)](https://go.dev/)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)
[![Go Report Card](https://goreportcard.com/badge/github.com/randilt/geckos3)](https://goreportcard.com/report/github.com/randilt/geckos3)

A lightweight S3-compatible object storage server that maps buckets to directories and objects to files on the local filesystem. Single binary, zero dependencies, pure Go.

**Key features:** Multipart uploads, custom metadata (`x-amz-meta-*`), standard HTTP headers (`Content-Encoding`, `Content-Disposition`, `Cache-Control`), SHA-256 payload verification, CORS support, SigV4 authentication (header + presigned URL), atomic writes, range requests, and structured JSON logging.

![geckos3](https://github.com/user-attachments/assets/59e3b56b-607e-4ffc-a1d5-2ad0c063214d)

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

### Download Pre-built Binary

Download the latest release for your platform from [GitHub Releases](https://github.com/randilt/geckos3/releases):

**Linux (x86_64):**

```bash
wget https://github.com/randilt/geckos3/releases/download/v0.4.0/geckos3_0.4.0_Linux_x86_64.tar.gz
tar -xzf geckos3_0.4.0_Linux_x86_64.tar.gz
chmod +x geckos3
./geckos3
```

**macOS (Apple Silicon):**

```bash
wget https://github.com/randilt/geckos3/releases/download/v0.4.0/geckos3_0.4.0_Darwin_arm64.tar.gz
tar -xzf geckos3_0.4.0_Darwin_arm64.tar.gz
chmod +x geckos3
./geckos3
```

**macOS (Intel):**

```bash
wget https://github.com/randilt/geckos3/releases/download/v0.4.0/geckos3_0.4.0_Darwin_x86_64.tar.gz
tar -xzf geckos3_0.4.0_Darwin_x86_64.tar.gz
chmod +x geckos3
./geckos3
```

**Windows (PowerShell):**

```powershell
Invoke-WebRequest -Uri "https://github.com/randilt/geckos3/releases/download/v0.4.0/geckos3_0.4.0_Windows_x86_64.tar.gz" -OutFile "geckos3.tar.gz"
tar -xzf geckos3.tar.gz
.\geckos3.exe
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

### Check Version

```bash
./geckos3 -version
```

### Install to System (Optional)

**Linux/macOS:**

```bash
sudo install -m 755 geckos3 /usr/local/bin/
# Now run from anywhere
geckos3
```

**Windows:** Add the directory containing `geckos3.exe` to your PATH environment variable.

## Configuration

All settings can be set via flags or environment variables:

| Flag          | Env Var                | Default      | Description                         |
| ------------- | ---------------------- | ------------ | ----------------------------------- |
| `-data-dir`   | `GECKOS3_DATA_DIR`     | `./data`     | Root directory for bucket storage   |
| `-listen`     | `GECKOS3_LISTEN`       | `:9000`      | HTTP listen address                 |
| `-access-key` | `GECKOS3_ACCESS_KEY`   | `geckoadmin` | AWS access key ID                   |
| `-secret-key` | `GECKOS3_SECRET_KEY`   | `geckoadmin` | AWS secret access key               |
| `-auth`       | `GECKOS3_AUTH_ENABLED` | `true`       | Enable/disable SigV4 authentication |
| `-metadata`   | `GECKOS3_METADATA`     | `true`       | Persist metadata in `.json` sidecar files |
| `-fsync`      | `GECKOS3_FSYNC`        | `false`      | Fsync files/dirs after writes (stronger durability) |

```bash
# Custom configuration
./geckos3 -data-dir=/mnt/storage -listen=:8080 -access-key=mykey -secret-key=mysecret

# Or with environment variables
export GECKOS3_ACCESS_KEY=mykey
export GECKOS3_SECRET_KEY=mysecret
./geckos3

# Disable auth for local development
./geckos3 -auth=false

# High-performance mode (disable metadata and enable no-fsync)
./geckos3 -metadata=false

# Strong durability mode (fsync every write)
./geckos3 -fsync=true
```

## Supported S3 Operations

| Operation               | Method   | Path                                           |
| ----------------------- | -------- | ---------------------------------------------- |
| ListBuckets             | `GET`    | `/`                                            |
| CreateBucket            | `PUT`    | `/{bucket}`                                    |
| DeleteBucket            | `DELETE` | `/{bucket}`                                    |
| HeadBucket              | `HEAD`   | `/{bucket}`                                    |
| ListObjectsV1           | `GET`    | `/{bucket}`                                    |
| ListObjectsV2           | `GET`    | `/{bucket}?list-type=2`                        |
| PutObject               | `PUT`    | `/{bucket}/{key}`                              |
| GetObject               | `GET`    | `/{bucket}/{key}`                              |
| HeadObject              | `HEAD`   | `/{bucket}/{key}`                              |
| DeleteObject            | `DELETE` | `/{bucket}/{key}`                              |
| CopyObject              | `PUT`    | `/{bucket}/{key}` + `x-amz-copy-source` header |
| DeleteObjects           | `POST`   | `/{bucket}?delete`                             |
| CreateMultipartUpload   | `POST`   | `/{bucket}/{key}?uploads`                      |
| UploadPart              | `PUT`    | `/{bucket}/{key}?partNumber={n}&uploadId={id}` |
| CompleteMultipartUpload | `POST`   | `/{bucket}/{key}?uploadId={id}`                |
| AbortMultipartUpload    | `DELETE` | `/{bucket}/{key}?uploadId={id}`                |

**ListObjectsV1** supports `prefix`, `delimiter`, `max-keys`, and `marker` parameters.

**ListObjectsV2** supports `prefix`, `delimiter`, `max-keys`, `start-after`, and `continuation-token` parameters. When `delimiter` is set, common prefixes are grouped and returned.

**CopyObject** is triggered by setting the `x-amz-copy-source` header (value: `/{source-bucket}/{source-key}`) on a PUT request. Content-Type is preserved from the source. The `x-amz-metadata-directive` header controls metadata handling: `COPY` (default) preserves source metadata, `REPLACE` uses the `Content-Type`, `Content-Encoding`, `Content-Disposition`, `Cache-Control`, and `x-amz-meta-*` headers from the PUT request instead.

**GetObject** supports HTTP `Range` requests for partial content retrieval.

**Content-Type** is preserved — the Content-Type sent during PUT is stored and returned on GET/HEAD.

**Custom Metadata** — Any `x-amz-meta-*` headers sent during PUT are stored and returned on GET/HEAD.

**Standard Headers** — `Content-Encoding`, `Content-Disposition`, and `Cache-Control` headers sent during PUT are stored and returned on GET/HEAD.

**Multipart Upload** — Create an upload with `POST ?uploads`, upload parts with `PUT ?partNumber=N&uploadId=X`, complete with `POST ?uploadId=X`, or abort with `DELETE ?uploadId=X`. Parts are staged on the filesystem and concatenated on completion. The multipart ETag follows the S3 convention (`md5-N`).

**Payload Verification** — When `X-Amz-Content-Sha256` is set to a hex SHA-256 digest (not `UNSIGNED-PAYLOAD`), the server verifies the payload matches and returns `400 BadDigest` on mismatch. This applies to both `PutObject` and `UploadPart`.

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

# With custom metadata and headers
s3.put_object(
    Bucket="mybucket",
    Key="report.pdf",
    Body=open("report.pdf", "rb"),
    ContentType="application/pdf",
    ContentDisposition='attachment; filename="report.pdf"',
    CacheControl="max-age=86400",
    Metadata={"author": "alice", "version": "2.0"},
)

# Multipart upload (handled automatically by boto3 for large files)
s3.upload_file("large-file.bin", "mybucket", "large-file.bin")
```

## Docker

### Pull from Docker Hub

```bash
# Latest version
docker pull randiltharusha/geckos3:latest

# Specific version
docker pull randiltharusha/geckos3:v0.4.0
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
- Metadata (ETag, Content-Type, custom headers, `x-amz-meta-*`) is stored in `.metadata.json` sidecar files (configurable via `-metadata`)
- Authentication uses AWS Signature Version 4 (header and presigned URL)
- All writes are atomic (temp file + rename); optional per-object fsync via `-fsync`
- Concurrent writes are protected by lock striping (256 fixed mutexes, FNV-1a hash selection) — network I/O runs outside the lock; only directory creation and rename are serialized
- CORS headers are included on every response; `OPTIONS` preflight requests are handled automatically for browser-based S3 clients
- Multipart uploads are staged in a hidden `.geckos3-multipart/` directory per bucket and excluded from object listings
- Abandoned multipart uploads are automatically garbage-collected after 24 hours by a background goroutine
- ListObjects is bounded to 100,000 scanned objects to prevent OOM on very large buckets
- Path traversal is blocked — keys that escape the data directory are rejected
- HTTP server enforces `ReadHeaderTimeout` (10s), `ReadTimeout` / `WriteTimeout` (6h for large uploads), and `IdleTimeout` (120s)

## Performance

GeckoS3 has a near-zero overhead translation layer between the S3 protocol and the local filesystem. It uses pure streaming I/O and lock striping to maximize concurrency.

### End-to-End S3 Throughput

Benchmark your own setup using [MinIO Warp](https://github.com/minio/warp):

```bash
make bench-warp
```

### Internal Memory Efficiency

Run Go-native benchmarks to verify allocations per operation:

```bash
make bench
```

Example output:

```text
BenchmarkPutObject-12           1364        6582083 ns/op       3585 B/op      51 allocs/op
BenchmarkGetObject-12         196150          21639 ns/op       2252 B/op      29 allocs/op
BenchmarkHTTPPutObject-12       1166        4173422 ns/op      44549 B/op     140 allocs/op
BenchmarkHTTPGetObject-12      14998         293912 ns/op       8899 B/op     112 allocs/op
```

### Performance Modes

GeckoS3 supports configurable performance trade-offs:

**Full Compatibility Mode (Default)**

```bash
./geckos3  # or ./geckos3 -metadata=true
```

- Content-Type, Content-Encoding, Content-Disposition, Cache-Control preserved
- Custom metadata (`x-amz-meta-*`) stored and returned
- ETags are consistent MD5 hashes
- Best for: production, serving files via HTTP, S3 clients requiring full metadata

**High-Performance Mode**

```bash
./geckos3 -metadata=false
```

- Skips writing `.metadata.json` sidecar files, reducing disk I/O
- Content-Type defaults to `application/octet-stream` on GET/HEAD
- Custom metadata not preserved across requests
- ETags are pseudo-hashes (size+mtime based)
- Best for: CI/CD pipelines, local testing, backup storage, temporary caches

**Strong Durability Mode**

```bash
./geckos3 -fsync=true
```

- Calls `fsync` on every written file and parent directory
- Guarantees data is flushed to durable storage before returning success
- Significantly slower writes (0.5–10ms overhead per object)
- Best for: critical data that must survive power loss

These flags can be combined: `-metadata=false -fsync=false` gives maximum throughput, while `-metadata=true -fsync=true` gives maximum compatibility and durability.

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
make bench           Run Go benchmarks
make bench-warp      Run E2E S3 Warp benchmarks
make clean           Remove build artifacts and data/
make docker-build    Build Docker image
make docker-run      Run in Docker
make docker-stop     Stop Docker container
make install         Install to /usr/local/bin
make fmt             Format code
make lint            Run go vet
```

## Limitations

- No versioning, lifecycle policies, or ACLs
- No TLS — use a reverse proxy (nginx, Caddy) for HTTPS
- No rate limiting — use a reverse proxy for rate limiting
- No upload size limit — relies on filesystem quotas
- Single-node only, no replication
- ListObjects scans up to 100,000 objects per bucket (returns error beyond this limit)

## License

Apache 2.0 - See [LICENSE](LICENSE) for details.
