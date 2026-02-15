#!/bin/bash
set -e

BENCH_PORT=9099

echo "🚀 Starting GeckoS3 Warp Benchmark..."

# Kill any stale server from a previous failed run
lsof -ti :$BENCH_PORT | xargs -r kill -9 2>/dev/null || true
sleep 1

# 1. Start geckos3 in the background with auth disabled for benchmarking
export GECKOS3_AUTH_ENABLED=false
export GECKOS3_DATA_DIR=/tmp/geckos3-bench-data
rm -rf $GECKOS3_DATA_DIR && mkdir -p $GECKOS3_DATA_DIR

./geckos3 -listen :$BENCH_PORT > /dev/null 2>&1 &
SERVER_PID=$!

# Ensure cleanup on exit (success or failure)
cleanup() {
  echo "🧹 Cleaning up..."
  kill $SERVER_PID 2>/dev/null || true
  rm -rf $GECKOS3_DATA_DIR
}
trap cleanup EXIT

# Give it a second to start
sleep 2

echo "✅ Server started (PID: $SERVER_PID)"
echo "⏱️  Running Warp Mixed Workload (This will take a few minutes)..."

# 2. Run Warp (Mixed workload: 15% PUT, 85% GET)
# Adjust --concurrent depending on your machine's CPU cores
warp mixed \
  --host=127.0.0.1:$BENCH_PORT \
  --access-key=benchmark \
  --secret-key=benchmark \
  --autoterm \
  --objects=5000 \
  --obj.size=1MiB \
  --concurrent=25 \
  --duration=1m \
  --insecure

echo "🎉 Benchmark complete!"
