#!/bin/bash
# Strata FIO Benchmark Suite
# Block-level I/O benchmarks for FUSE-mounted Strata filesystem
#
# Usage: ./fio_benchmark.sh [MOUNT_PATH]
#
# Requires: Strata FUSE mount, fio

set -euo pipefail

MOUNT_PATH="${1:-/mnt/strata}"
OUTPUT_DIR="${OUTPUT_DIR:-./results}"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}  Strata FIO Benchmark Suite${NC}"
echo -e "${GREEN}========================================${NC}"
echo ""
echo "Mount Path: ${MOUNT_PATH}"
echo "Output:     ${OUTPUT_DIR}"
echo ""

# Check mount
if ! mountpoint -q "${MOUNT_PATH}" 2>/dev/null; then
    echo "Warning: ${MOUNT_PATH} may not be a mount point"
fi

mkdir -p "${OUTPUT_DIR}"

# Sequential read benchmark
run_seq_read() {
    echo -e "${YELLOW}Sequential Read Benchmark...${NC}"
    fio --name=seq_read \
        --directory="${MOUNT_PATH}" \
        --rw=read \
        --bs=1M \
        --size=1G \
        --numjobs=1 \
        --time_based \
        --runtime=60 \
        --group_reporting \
        --output-format=json \
        --output="${OUTPUT_DIR}/fio_seq_read_${TIMESTAMP}.json"
}

# Sequential write benchmark
run_seq_write() {
    echo -e "${YELLOW}Sequential Write Benchmark...${NC}"
    fio --name=seq_write \
        --directory="${MOUNT_PATH}" \
        --rw=write \
        --bs=1M \
        --size=1G \
        --numjobs=1 \
        --time_based \
        --runtime=60 \
        --group_reporting \
        --output-format=json \
        --output="${OUTPUT_DIR}/fio_seq_write_${TIMESTAMP}.json"
}

# Random read benchmark (4K)
run_rand_read() {
    echo -e "${YELLOW}Random Read Benchmark (4K)...${NC}"
    fio --name=rand_read \
        --directory="${MOUNT_PATH}" \
        --rw=randread \
        --bs=4K \
        --size=1G \
        --numjobs=4 \
        --iodepth=32 \
        --time_based \
        --runtime=60 \
        --group_reporting \
        --output-format=json \
        --output="${OUTPUT_DIR}/fio_rand_read_${TIMESTAMP}.json"
}

# Random write benchmark (4K)
run_rand_write() {
    echo -e "${YELLOW}Random Write Benchmark (4K)...${NC}"
    fio --name=rand_write \
        --directory="${MOUNT_PATH}" \
        --rw=randwrite \
        --bs=4K \
        --size=1G \
        --numjobs=4 \
        --iodepth=32 \
        --time_based \
        --runtime=60 \
        --group_reporting \
        --output-format=json \
        --output="${OUTPUT_DIR}/fio_rand_write_${TIMESTAMP}.json"
}

# Mixed workload benchmark
run_mixed() {
    echo -e "${YELLOW}Mixed Read/Write Benchmark (70/30)...${NC}"
    fio --name=mixed \
        --directory="${MOUNT_PATH}" \
        --rw=randrw \
        --rwmixread=70 \
        --bs=4K \
        --size=1G \
        --numjobs=4 \
        --iodepth=32 \
        --time_based \
        --runtime=60 \
        --group_reporting \
        --output-format=json \
        --output="${OUTPUT_DIR}/fio_mixed_${TIMESTAMP}.json"
}

# Run all benchmarks
run_seq_read
run_seq_write
run_rand_read
run_rand_write
run_mixed

echo ""
echo -e "${GREEN}FIO benchmarks complete!${NC}"
echo "Results saved to: ${OUTPUT_DIR}/"
