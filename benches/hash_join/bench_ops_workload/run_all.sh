#!/bin/bash

# Exit immediately if a command exits with a non-zero status
set -e

# Get the absolute path of the directory containing the script
SCRIPT_DIR="$(cd "$(dirname "$0")"; pwd)"
echo "Script directory: $SCRIPT_DIR"

# Run the data generation script
echo "Running data generation script..."
bash "$SCRIPT_DIR/run_gen.sh"

# Cargo build
# cargo build --release

# Run the benchmark script
echo "Running benchmark script..."
bash "$SCRIPT_DIR/run_bench.sh"

echo "All tasks completed."