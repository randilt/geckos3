#!/bin/bash
set -e

# ============================================================================
# GeckoS3 Comprehensive Benchmark Suite
# ============================================================================
# This script runs multiple Warp scenarios to thoroughly test S3 performance:
# 1. Mixed workload (realistic usage)
# 2. GET-heavy workload (CDN/download scenario)
# 3. PUT-heavy workload (backup/upload scenario)
# 4. Small files (1KB - many operations)
# 5. Large files (10MB - bandwidth test)
# 6. Metadata comparison (with/without metadata persistence)
# ============================================================================

BENCH_PORT=9099
DATA_DIR=/tmp/geckos3-bench-data
RESULTS_DIR=./benchmark-results
TIMESTAMP=$(date +%Y%m%d-%H%M%S)

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Create results directory
mkdir -p "$RESULTS_DIR"

echo -e "${BLUE}╔════════════════════════════════════════════════════════════════╗${NC}"
echo -e "${BLUE}║           GeckoS3 Comprehensive Benchmark Suite                ║${NC}"
echo -e "${BLUE}╚════════════════════════════════════════════════════════════════╝${NC}"
echo ""

# ============================================================================
# Helper Functions
# ============================================================================

cleanup_server() {
    if [ -n "$SERVER_PID" ] && kill -0 "$SERVER_PID" 2>/dev/null; then
        echo -e "${YELLOW}🧹 Stopping server (PID: $SERVER_PID)...${NC}"
        kill "$SERVER_PID" 2>/dev/null || true
        wait "$SERVER_PID" 2>/dev/null || true
    fi
    rm -rf "$DATA_DIR"
}

start_server() {
    local metadata_enabled=$1
    local label=$2
    
    # Kill any stale processes
    lsof -ti :$BENCH_PORT | xargs -r kill -9 2>/dev/null || true
    sleep 1
    
    # Clean data directory
    rm -rf "$DATA_DIR" && mkdir -p "$DATA_DIR"
    
    # Start server
    export GECKOS3_AUTH_ENABLED=false
    export GECKOS3_DATA_DIR="$DATA_DIR"
    export GECKOS3_METADATA="$metadata_enabled"
    
    ./geckos3 -listen ":$BENCH_PORT" > /dev/null 2>&1 &
    SERVER_PID=$!
    
    sleep 2
    
    if ! kill -0 "$SERVER_PID" 2>/dev/null; then
        echo -e "${RED}✗ Failed to start server${NC}"
        exit 1
    fi
    
    echo -e "${GREEN}✓ Server started${NC} (PID: $SERVER_PID, metadata=$metadata_enabled) $label"
}

run_warp_test() {
    local test_name=$1
    local output_file="$RESULTS_DIR/${TIMESTAMP}-${test_name}.txt"
    shift
    
    echo -e "${BLUE}▶ Running: $test_name${NC}"
    echo "  Output: $output_file"
    
    # Run warp and capture output
    warp "$@" \
        --host=127.0.0.1:$BENCH_PORT \
        --access-key=benchmark \
        --secret-key=benchmark \
        --autoterm \
        --insecure \
        2>&1 | tee "$output_file"
    
    echo ""
}

extract_metric() {
    local file=$1
    local pattern=$2
    grep "$pattern" "$file" | head -1 || echo "N/A"
}

print_summary() {
    echo -e "${GREEN}╔════════════════════════════════════════════════════════════════╗${NC}"
    echo -e "${GREEN}║                    BENCHMARK SUMMARY                           ║${NC}"
    echo -e "${GREEN}╚════════════════════════════════════════════════════════════════╝${NC}"
    echo ""
    
    for result_file in "$RESULTS_DIR"/${TIMESTAMP}-*.txt; do
        if [ -f "$result_file" ]; then
            local test_name=$(basename "$result_file" .txt | sed "s/${TIMESTAMP}-//")
            echo -e "${YELLOW}📊 $test_name${NC}"
            
            # Extract key metrics
            extract_metric "$result_file" "Total.*Average:"
            extract_metric "$result_file" "GET.*Average:"
            extract_metric "$result_file" "PUT.*Average:"
            echo ""
        fi
    done
    
    echo -e "${BLUE}Results saved in: $RESULTS_DIR/${NC}"
    echo ""
}

# Trap cleanup
trap cleanup_server EXIT INT TERM

# ============================================================================
# System Information
# ============================================================================

echo -e "${BLUE}📋 System Information${NC}"
echo "-------------------"
echo "Hostname: $(hostname)"
echo "OS: $(uname -s) $(uname -r)"
echo "CPU: $(grep -m1 'model name' /proc/cpuinfo | cut -d: -f2 | xargs || echo 'N/A')"
echo "Cores: $(nproc)"
echo "Memory: $(free -h | awk '/^Mem:/ {print $2}')"
echo ""

# Check disk speed
echo -e "${BLUE}💾 Testing Disk Performance${NC}"
echo "----------------------------"
DISK_TEST_FILE="$DATA_DIR/disk-test.tmp"
mkdir -p "$DATA_DIR"

# Write test
echo -n "Write speed: "
dd if=/dev/zero of="$DISK_TEST_FILE" bs=1G count=1 oflag=direct 2>&1 | \
    grep -o '[0-9.]* [GM]B/s' | tail -1

# Read test
echo -n "Read speed:  "
dd if="$DISK_TEST_FILE" of=/dev/null bs=1G count=1 iflag=direct 2>&1 | \
    grep -o '[0-9.]* [GM]B/s' | tail -1

rm -f "$DISK_TEST_FILE"
echo ""

# Check warp version
echo -e "${BLUE}🔧 Tool Versions${NC}"
echo "----------------"
echo "GeckoS3: $(./geckos3 -version 2>&1 | head -1)"
echo "Warp: $(warp --version 2>&1 | head -1 || echo 'Not found')"
echo ""
echo ""

# ============================================================================
# Benchmark 1: Mixed Workload (Baseline)
# ============================================================================

echo -e "${GREEN}═══════════════════════════════════════════════════════════════${NC}"
echo -e "${GREEN}  TEST 1: Mixed Workload (Baseline - Metadata Enabled)${NC}"
echo -e "${GREEN}═══════════════════════════════════════════════════════════════${NC}"
echo ""

start_server "true" "[realistic usage pattern]"

run_warp_test "01-mixed-baseline" \
    mixed \
    --objects=5000 \
    --obj.size=1MiB \
    --concurrent=25 \
    --duration=1m

cleanup_server

# ============================================================================
# Benchmark 2: Mixed Workload (No Metadata)
# ============================================================================

echo -e "${GREEN}═══════════════════════════════════════════════════════════════${NC}"
echo -e "${GREEN}  TEST 2: Mixed Workload (Metadata Disabled)${NC}"
echo -e "${GREEN}═══════════════════════════════════════════════════════════════${NC}"
echo ""

start_server "false" "[high-performance mode]"

run_warp_test "02-mixed-no-metadata" \
    mixed \
    --objects=5000 \
    --obj.size=1MiB \
    --concurrent=25 \
    --duration=1m

cleanup_server

# ============================================================================
# Benchmark 3: GET-Heavy Workload
# ============================================================================

echo -e "${GREEN}═══════════════════════════════════════════════════════════════${NC}"
echo -e "${GREEN}  TEST 3: GET-Heavy Workload (95% reads)${NC}"
echo -e "${GREEN}═══════════════════════════════════════════════════════════════${NC}"
echo ""

start_server "true" "[CDN/download scenario]"

run_warp_test "03-get-heavy" \
    get \
    --objects=5000 \
    --obj.size=1MiB \
    --concurrent=50 \
    --duration=1m

cleanup_server

# ============================================================================
# Benchmark 4: PUT-Heavy Workload
# ============================================================================

echo -e "${GREEN}═══════════════════════════════════════════════════════════════${NC}"
echo -e "${GREEN}  TEST 4: PUT-Heavy Workload (95% writes)${NC}"
echo -e "${GREEN}═══════════════════════════════════════════════════════════════${NC}"
echo ""

start_server "true" "[backup/upload scenario]"

run_warp_test "04-put-heavy" \
    put \
    --objects=3000 \
    --obj.size=1MiB \
    --concurrent=25 \
    --duration=1m

cleanup_server

# ============================================================================
# Benchmark 5: Small Files (Many Operations)
# ============================================================================

echo -e "${GREEN}═══════════════════════════════════════════════════════════════${NC}"
echo -e "${GREEN}  TEST 5: Small Files (1KB objects)${NC}"
echo -e "${GREEN}═══════════════════════════════════════════════════════════════${NC}"
echo ""

start_server "true" "[metadata/logging heavy]"

run_warp_test "05-small-files" \
    mixed \
    --objects=10000 \
    --obj.size=1KiB \
    --concurrent=25 \
    --duration=1m

cleanup_server

# ============================================================================
# Benchmark 6: Large Files (Bandwidth Test)
# ============================================================================

echo -e "${GREEN}═══════════════════════════════════════════════════════════════${NC}"
echo -e "${GREEN}  TEST 6: Large Files (10MB objects)${NC}"
echo -e "${GREEN}═══════════════════════════════════════════════════════════════${NC}"
echo ""

start_server "true" "[streaming/bandwidth test]"

run_warp_test "06-large-files" \
    mixed \
    --objects=500 \
    --obj.size=10MiB \
    --concurrent=10 \
    --duration=1m

cleanup_server

# ============================================================================
# Benchmark 7: High Concurrency Stress Test
# ============================================================================

echo -e "${GREEN}═══════════════════════════════════════════════════════════════${NC}"
echo -e "${GREEN}  TEST 7: High Concurrency (100 parallel clients)${NC}"
echo -e "${GREEN}═══════════════════════════════════════════════════════════════${NC}"
echo ""

start_server "true" "[lock contention test]"

run_warp_test "07-high-concurrency" \
    mixed \
    --objects=5000 \
    --obj.size=1MiB \
    --concurrent=100 \
    --duration=1m

cleanup_server

# ============================================================================
# Benchmark 8: DELETE Performance
# ============================================================================

echo -e "${GREEN}═══════════════════════════════════════════════════════════════${NC}"
echo -e "${GREEN}  TEST 8: DELETE Operations${NC}"
echo -e "${GREEN}═══════════════════════════════════════════════════════════════${NC}"
echo ""

start_server "true" "[cleanup scenario]"

run_warp_test "08-delete" \
    delete \
    --objects=5000 \
    --obj.size=1MiB \
    --concurrent=25 \
    --duration=1m

cleanup_server

# ============================================================================
# Benchmark 9: LIST Performance
# ============================================================================

echo -e "${GREEN}═══════════════════════════════════════════════════════════════${NC}"
echo -e "${GREEN}  TEST 9: LIST Operations${NC}"
echo -e "${GREEN}═══════════════════════════════════════════════════════════════${NC}"
echo ""

start_server "true" "[directory traversal]"

# First, populate with objects
echo "  Populating bucket with 10,000 objects..."
warp put \
    --host=127.0.0.1:$BENCH_PORT \
    --access-key=benchmark \
    --secret-key=benchmark \
    --objects=10000 \
    --obj.size=1KiB \
    --concurrent=50 \
    --autoterm \
    --insecure \
    > /dev/null 2>&1

run_warp_test "09-list" \
    list \
    --objects=10000 \
    --concurrent=10 \
    --duration=30s

cleanup_server

# ============================================================================
# Generate Summary Report
# ============================================================================

print_summary

# ============================================================================
# Generate Comparison Chart (ASCII)
# ============================================================================

echo -e "${BLUE}╔════════════════════════════════════════════════════════════════╗${NC}"
echo -e "${BLUE}║                  PERFORMANCE COMPARISON                        ║${NC}"
echo -e "${BLUE}╚════════════════════════════════════════════════════════════════╝${NC}"
echo ""

# Parse results and create comparison
BASELINE_FILE="$RESULTS_DIR/${TIMESTAMP}-01-mixed-baseline.txt"
NO_META_FILE="$RESULTS_DIR/${TIMESTAMP}-02-mixed-no-metadata.txt"

if [ -f "$BASELINE_FILE" ] && [ -f "$NO_META_FILE" ]; then
    echo -e "${YELLOW}Mixed Workload Comparison:${NC}"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    
    baseline_total=$(grep "Total.*Average:" "$BASELINE_FILE" | grep -oP '\d+\.\d+ MiB/s' | head -1)
    no_meta_total=$(grep "Total.*Average:" "$NO_META_FILE" | grep -oP '\d+\.\d+ MiB/s' | head -1)
    
    baseline_get=$(grep "GET.*Average:" "$BASELINE_FILE" | grep -oP '\d+\.\d+ MiB/s' | head -1)
    no_meta_get=$(grep "GET.*Average:" "$NO_META_FILE" | grep -oP '\d+\.\d+ MiB/s' | head -1)
    
    baseline_put=$(grep "PUT.*Average:" "$BASELINE_FILE" | grep -oP '\d+\.\d+ MiB/s' | head -1)
    no_meta_put=$(grep "PUT.*Average:" "$NO_META_FILE" | grep -oP '\d+\.\d+ MiB/s' | head -1)
    
    printf "%-25s %15s %15s\n" "Metric" "With Metadata" "Without Metadata"
    printf "%-25s %15s %15s\n" "------" "-------------" "----------------"
    printf "%-25s %15s %15s\n" "Total Throughput" "$baseline_total" "$no_meta_total"
    printf "%-25s %15s %15s\n" "GET Throughput" "$baseline_get" "$no_meta_get"
    printf "%-25s %15s %15s\n" "PUT Throughput" "$baseline_put" "$no_meta_put"
    
    echo ""
fi

# ============================================================================
# Generate CSV Report
# ============================================================================

CSV_FILE="$RESULTS_DIR/${TIMESTAMP}-summary.csv"
echo "test_name,total_throughput,get_throughput,put_throughput,delete_throughput" > "$CSV_FILE"

for result_file in "$RESULTS_DIR"/${TIMESTAMP}-*.txt; do
    if [ -f "$result_file" ]; then
        test_name=$(basename "$result_file" .txt | sed "s/${TIMESTAMP}-//")
        
        total=$(grep "Total.*Average:" "$result_file" | grep -oP '\d+\.\d+' | head -1 || echo "0")
        get=$(grep "GET.*Average:" "$result_file" | grep -oP '\d+\.\d+ MiB/s' | head -1 | grep -oP '\d+\.\d+' || echo "0")
        put=$(grep "PUT.*Average:" "$result_file" | grep -oP '\d+\.\d+ MiB/s' | head -1 | grep -oP '\d+\.\d+' || echo "0")
        delete=$(grep "DELETE.*Average:" "$result_file" | grep -oP '\d+\.\d+ obj/s' | head -1 | grep -oP '\d+\.\d+' || echo "0")
        
        echo "$test_name,$total,$get,$put,$delete" >> "$CSV_FILE"
    fi
done

echo -e "${GREEN}✓ CSV report saved: $CSV_FILE${NC}"
echo ""

# ============================================================================
# Recommendations
# ============================================================================

echo -e "${BLUE}╔════════════════════════════════════════════════════════════════╗${NC}"
echo -e "${BLUE}║                     RECOMMENDATIONS                            ║${NC}"
echo -e "${BLUE}╚════════════════════════════════════════════════════════════════╝${NC}"
echo ""

if [ -f "$BASELINE_FILE" ] && [ -f "$NO_META_FILE" ]; then
    baseline_put_val=$(grep "PUT.*Average:" "$BASELINE_FILE" | grep -oP '\d+\.\d+' | head -1)
    no_meta_put_val=$(grep "PUT.*Average:" "$NO_META_FILE" | grep -oP '\d+\.\d+' | head -1)
    
    if [ -n "$baseline_put_val" ] && [ -n "$no_meta_put_val" ]; then
        improvement=$(echo "scale=1; ($no_meta_put_val - $baseline_put_val) / $baseline_put_val * 100" | bc)
        
        echo -e "📊 Metadata Impact: ${GREEN}+${improvement}%${NC} throughput gain when disabled"
        echo ""
        
        if (( $(echo "$improvement > 50" | bc -l) )); then
            echo "💡 Consider:"
            echo "   • Use -metadata=false for CI/testing environments"
            echo "   • Keep -metadata=true for production use"
            echo "   • Document the trade-off in your README"
        else
            echo "💡 Metadata overhead is minimal - keep enabled for S3 compliance"
        fi
        echo ""
    fi
fi

echo -e "${GREEN}╔════════════════════════════════════════════════════════════════╗${NC}"
echo -e "${GREEN}║                  BENCHMARK COMPLETE! 🎉                        ║${NC}"
echo -e "${GREEN}╚════════════════════════════════════════════════════════════════╝${NC}"