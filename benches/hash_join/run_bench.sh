#!/bin/bash

# Exit immediately if a command exits with a non-zero status
set -e

# Get the absolute path of the directory containing the script
SCRIPT_DIR="$(cd "$(dirname "$0")"; pwd)"
echo "Script directory: $SCRIPT_DIR"

# Build absolute paths to data.csv, ops.csv, and expected data file
DATA_CSV="$SCRIPT_DIR/data.csv"
OPS_CSV="$SCRIPT_DIR/ops.csv"
EXPECTED_DATA_CSV="$SCRIPT_DIR/recent_data_after_ops.csv"

# Assuming the Rust project root is two directories up from the script directory
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../../"; pwd)"
echo "Project root directory: $PROJECT_ROOT"

# Change to the project root directory
cd "$PROJECT_ROOT"

# Build the Rust project
# echo "Building the Rust project..."
# cargo build --release

# Run the benchmark
echo "Running the Rust benchmark..."
cargo run --release --bin hash_join_bench -- "$DATA_CSV" "$OPS_CSV" "$EXPECTED_DATA_CSV"

echo "Benchmark completed."
