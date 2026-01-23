#!/bin/bash
# Strata Benchmark Suite
# Comprehensive performance testing for Strata distributed file system
#
# Usage: ./run_benchmarks.sh [OPTIONS]
#
# Options:
#   --endpoint URL    S3 endpoint (default: http://localhost:8080)
#   --output DIR      Output directory (default: ./results)
#   --quick           Run quick benchmarks only
#   --full            Run full benchmark suite
#   --compare FILE    Compare with previous results

set -euo pipefail

# Default configuration
ENDPOINT="${STRATA_ENDPOINT:-http://localhost:8080}"
OUTPUT_DIR="${OUTPUT_DIR:-./results}"
ACCESS_KEY="${STRATA_ACCESS_KEY:-strata-access-key}"
SECRET_KEY="${STRATA_SECRET_KEY:-strata-secret-key}"
BUCKET="strata-benchmark"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
RESULTS_FILE="${OUTPUT_DIR}/benchmark_${TIMESTAMP}.json"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Parse arguments
QUICK_MODE=false
FULL_MODE=false
COMPARE_FILE=""

while [[ $# -gt 0 ]]; do
    case $1 in
        --endpoint)
            ENDPOINT="$2"
            shift 2
            ;;
        --output)
            OUTPUT_DIR="$2"
            shift 2
            ;;
        --quick)
            QUICK_MODE=true
            shift
            ;;
        --full)
            FULL_MODE=true
            shift
            ;;
        --compare)
            COMPARE_FILE="$2"
            shift 2
            ;;
        *)
            echo "Unknown option: $1"
            exit 1
            ;;
    esac
done

# Create output directory
mkdir -p "${OUTPUT_DIR}"

echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}  Strata Benchmark Suite${NC}"
echo -e "${GREEN}========================================${NC}"
echo ""
echo "Endpoint:    ${ENDPOINT}"
echo "Output:      ${OUTPUT_DIR}"
echo "Timestamp:   ${TIMESTAMP}"
echo ""

# Check dependencies
check_dependencies() {
    local missing=()

    command -v aws >/dev/null 2>&1 || missing+=("aws-cli")
    command -v fio >/dev/null 2>&1 || missing+=("fio")
    command -v jq >/dev/null 2>&1 || missing+=("jq")

    if [ ${#missing[@]} -ne 0 ]; then
        echo -e "${RED}Missing dependencies: ${missing[*]}${NC}"
        echo "Install with:"
        echo "  brew install awscli fio jq  # macOS"
        echo "  apt install awscli fio jq   # Ubuntu/Debian"
        exit 1
    fi
}

# Configure AWS CLI for Strata
configure_aws() {
    export AWS_ACCESS_KEY_ID="${ACCESS_KEY}"
    export AWS_SECRET_ACCESS_KEY="${SECRET_KEY}"
    export AWS_DEFAULT_REGION="us-east-1"
}

# Create benchmark bucket
setup_bucket() {
    echo -e "${YELLOW}Setting up benchmark bucket...${NC}"
    aws --endpoint-url "${ENDPOINT}" s3 mb "s3://${BUCKET}" 2>/dev/null || true
}

# S3 benchmark using aws s3 commands
run_s3_benchmark() {
    local name=$1
    local size=$2
    local count=$3
    local concurrency=${4:-1}

    echo -e "${YELLOW}Running: ${name}${NC}"

    # Generate test data
    local testfile="/tmp/strata_bench_${size}"
    dd if=/dev/urandom of="${testfile}" bs="${size}" count=1 2>/dev/null

    local start_time=$(date +%s.%N)

    # Upload files
    for i in $(seq 1 "${count}"); do
        aws --endpoint-url "${ENDPOINT}" s3 cp "${testfile}" "s3://${BUCKET}/bench_${name}_${i}" --quiet &

        # Limit concurrency
        if (( i % concurrency == 0 )); then
            wait
        fi
    done
    wait

    local upload_time=$(echo "$(date +%s.%N) - ${start_time}" | bc)
    local upload_throughput=$(echo "scale=2; (${count} * ${size}) / ${upload_time} / 1024 / 1024" | bc)

    # Download files
    start_time=$(date +%s.%N)
    for i in $(seq 1 "${count}"); do
        aws --endpoint-url "${ENDPOINT}" s3 cp "s3://${BUCKET}/bench_${name}_${i}" "/tmp/download_${i}" --quiet &

        if (( i % concurrency == 0 )); then
            wait
        fi
    done
    wait

    local download_time=$(echo "$(date +%s.%N) - ${start_time}" | bc)
    local download_throughput=$(echo "scale=2; (${count} * ${size}) / ${download_time} / 1024 / 1024" | bc)

    # Cleanup
    for i in $(seq 1 "${count}"); do
        aws --endpoint-url "${ENDPOINT}" s3 rm "s3://${BUCKET}/bench_${name}_${i}" --quiet &
        rm -f "/tmp/download_${i}"
    done
    wait
    rm -f "${testfile}"

    echo "  Upload:   ${upload_throughput} MB/s (${upload_time}s)"
    echo "  Download: ${download_throughput} MB/s (${download_time}s)"

    # Output JSON result
    cat >> "${RESULTS_FILE}.tmp" <<EOF
    {
      "name": "${name}",
      "size_bytes": ${size},
      "count": ${count},
      "concurrency": ${concurrency},
      "upload_throughput_mbps": ${upload_throughput},
      "upload_time_seconds": ${upload_time},
      "download_throughput_mbps": ${download_throughput},
      "download_time_seconds": ${download_time}
    },
EOF
}

# Latency benchmark
run_latency_benchmark() {
    echo -e "${YELLOW}Running latency benchmark...${NC}"

    local iterations=100
    local testfile="/tmp/strata_latency_test"
    echo "test" > "${testfile}"

    local total_put=0
    local total_get=0
    local total_delete=0

    for i in $(seq 1 "${iterations}"); do
        # PUT latency
        local start=$(date +%s.%N)
        aws --endpoint-url "${ENDPOINT}" s3 cp "${testfile}" "s3://${BUCKET}/latency_test" --quiet
        local put_time=$(echo "$(date +%s.%N) - ${start}" | bc)
        total_put=$(echo "${total_put} + ${put_time}" | bc)

        # GET latency
        start=$(date +%s.%N)
        aws --endpoint-url "${ENDPOINT}" s3 cp "s3://${BUCKET}/latency_test" "/tmp/latency_download" --quiet
        local get_time=$(echo "$(date +%s.%N) - ${start}" | bc)
        total_get=$(echo "${total_get} + ${get_time}" | bc)

        # DELETE latency
        start=$(date +%s.%N)
        aws --endpoint-url "${ENDPOINT}" s3 rm "s3://${BUCKET}/latency_test" --quiet
        local delete_time=$(echo "$(date +%s.%N) - ${start}" | bc)
        total_delete=$(echo "${total_delete} + ${delete_time}" | bc)
    done

    local avg_put=$(echo "scale=3; ${total_put} / ${iterations} * 1000" | bc)
    local avg_get=$(echo "scale=3; ${total_get} / ${iterations} * 1000" | bc)
    local avg_delete=$(echo "scale=3; ${total_delete} / ${iterations} * 1000" | bc)

    echo "  PUT:    ${avg_put} ms"
    echo "  GET:    ${avg_get} ms"
    echo "  DELETE: ${avg_delete} ms"

    rm -f "${testfile}" "/tmp/latency_download"

    cat >> "${RESULTS_FILE}.tmp" <<EOF
    {
      "name": "latency",
      "iterations": ${iterations},
      "avg_put_ms": ${avg_put},
      "avg_get_ms": ${avg_get},
      "avg_delete_ms": ${avg_delete}
    },
EOF
}

# List operations benchmark
run_list_benchmark() {
    echo -e "${YELLOW}Running list operations benchmark...${NC}"

    local object_count=1000
    local testfile="/tmp/strata_list_test"
    echo "x" > "${testfile}"

    # Create objects
    echo "  Creating ${object_count} objects..."
    for i in $(seq 1 "${object_count}"); do
        aws --endpoint-url "${ENDPOINT}" s3 cp "${testfile}" "s3://${BUCKET}/list_test/obj_${i}" --quiet &
        if (( i % 50 == 0 )); then
            wait
        fi
    done
    wait

    # List benchmark
    local start=$(date +%s.%N)
    local count=$(aws --endpoint-url "${ENDPOINT}" s3 ls "s3://${BUCKET}/list_test/" | wc -l)
    local list_time=$(echo "$(date +%s.%N) - ${start}" | bc)

    echo "  List ${count} objects: ${list_time}s"

    # Cleanup
    aws --endpoint-url "${ENDPOINT}" s3 rm "s3://${BUCKET}/list_test/" --recursive --quiet
    rm -f "${testfile}"

    cat >> "${RESULTS_FILE}.tmp" <<EOF
    {
      "name": "list_operations",
      "object_count": ${object_count},
      "list_time_seconds": ${list_time}
    },
EOF
}

# Multipart upload benchmark
run_multipart_benchmark() {
    echo -e "${YELLOW}Running multipart upload benchmark...${NC}"

    local size=$((100 * 1024 * 1024))  # 100MB
    local testfile="/tmp/strata_multipart_test"
    dd if=/dev/urandom of="${testfile}" bs=1M count=100 2>/dev/null

    local start=$(date +%s.%N)
    aws --endpoint-url "${ENDPOINT}" s3 cp "${testfile}" "s3://${BUCKET}/multipart_test" --quiet
    local upload_time=$(echo "$(date +%s.%N) - ${start}" | bc)
    local throughput=$(echo "scale=2; 100 / ${upload_time}" | bc)

    echo "  100MB multipart upload: ${upload_time}s (${throughput} MB/s)"

    # Cleanup
    aws --endpoint-url "${ENDPOINT}" s3 rm "s3://${BUCKET}/multipart_test" --quiet
    rm -f "${testfile}"

    cat >> "${RESULTS_FILE}.tmp" <<EOF
    {
      "name": "multipart_upload",
      "size_mb": 100,
      "upload_time_seconds": ${upload_time},
      "throughput_mbps": ${throughput}
    },
EOF
}

# Generate results JSON
generate_results() {
    echo -e "${YELLOW}Generating results...${NC}"

    # Create final JSON
    cat > "${RESULTS_FILE}" <<EOF
{
  "timestamp": "${TIMESTAMP}",
  "endpoint": "${ENDPOINT}",
  "benchmarks": [
$(cat "${RESULTS_FILE}.tmp" | sed '$ s/,$//')
  ]
}
EOF

    rm -f "${RESULTS_FILE}.tmp"
    echo -e "${GREEN}Results saved to: ${RESULTS_FILE}${NC}"
}

# Compare results
compare_results() {
    if [ -n "${COMPARE_FILE}" ] && [ -f "${COMPARE_FILE}" ]; then
        echo -e "${YELLOW}Comparing with: ${COMPARE_FILE}${NC}"
        echo ""

        # Extract and compare key metrics
        local old_upload=$(jq -r '.benchmarks[] | select(.name=="1MB_sequential") | .upload_throughput_mbps' "${COMPARE_FILE}")
        local new_upload=$(jq -r '.benchmarks[] | select(.name=="1MB_sequential") | .upload_throughput_mbps' "${RESULTS_FILE}")

        if [ -n "${old_upload}" ] && [ -n "${new_upload}" ]; then
            local diff=$(echo "scale=1; ((${new_upload} - ${old_upload}) / ${old_upload}) * 100" | bc)
            echo "1MB Upload Throughput: ${old_upload} -> ${new_upload} MB/s (${diff}%)"
        fi
    fi
}

# Main execution
main() {
    check_dependencies
    configure_aws
    setup_bucket

    # Initialize temp results file
    echo "" > "${RESULTS_FILE}.tmp"

    if [ "${QUICK_MODE}" = true ]; then
        echo -e "${GREEN}Running quick benchmarks...${NC}"
        echo ""
        run_s3_benchmark "1KB_sequential" 1024 100 1
        run_s3_benchmark "1MB_sequential" 1048576 10 1
        run_latency_benchmark
    elif [ "${FULL_MODE}" = true ]; then
        echo -e "${GREEN}Running full benchmark suite...${NC}"
        echo ""

        # Sequential benchmarks
        run_s3_benchmark "1KB_sequential" 1024 1000 1
        run_s3_benchmark "64KB_sequential" 65536 500 1
        run_s3_benchmark "1MB_sequential" 1048576 100 1
        run_s3_benchmark "10MB_sequential" 10485760 20 1

        # Concurrent benchmarks
        run_s3_benchmark "1KB_concurrent_10" 1024 1000 10
        run_s3_benchmark "1MB_concurrent_10" 1048576 100 10
        run_s3_benchmark "1MB_concurrent_50" 1048576 100 50

        # Other benchmarks
        run_latency_benchmark
        run_list_benchmark
        run_multipart_benchmark
    else
        echo -e "${GREEN}Running standard benchmarks...${NC}"
        echo ""
        run_s3_benchmark "1KB_sequential" 1024 500 1
        run_s3_benchmark "64KB_sequential" 65536 200 1
        run_s3_benchmark "1MB_sequential" 1048576 50 1
        run_s3_benchmark "1MB_concurrent_10" 1048576 50 10
        run_latency_benchmark
    fi

    generate_results
    compare_results

    echo ""
    echo -e "${GREEN}Benchmark complete!${NC}"
}

main "$@"
