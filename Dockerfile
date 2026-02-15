FROM golang:1.23-alpine AS builder

WORKDIR /build

# Copy source files
COPY *.go go.mod ./

# Build statically linked binary
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o geckos3

# Runtime stage
FROM alpine:latest

RUN apk --no-cache add ca-certificates

WORKDIR /app

# Copy binary from builder
COPY --from=builder /build/geckos3 .

# Create data directory
RUN mkdir -p /data

# Volume for persistent storage
VOLUME ["/data"]

# Expose port
EXPOSE 9000

# Default configuration
ENV S3LITE_DATA_DIR=/data
ENV S3LITE_LISTEN=:9000
ENV S3LITE_ACCESS_KEY=minioadmin
ENV S3LITE_SECRET_KEY=minioadmin
ENV S3LITE_AUTH_ENABLED=true

# Run server
CMD ["./geckos3"]