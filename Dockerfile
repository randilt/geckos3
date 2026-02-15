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

# Default configuration (overridable at runtime)
# Use -e flag or docker-compose environment to set:
#   GECKOS3_DATA_DIR (default: ./data)
#   GECKOS3_LISTEN (default: :9000)
#   GECKOS3_ACCESS_KEY (default: geckoadmin)
#   GECKOS3_SECRET_KEY (default: geckoadmin)
#   GECKOS3_AUTH_ENABLED (default: true)
ENV GECKOS3_DATA_DIR=/data \
    GECKOS3_LISTEN=:9000

# Run server
CMD ["./geckos3"]