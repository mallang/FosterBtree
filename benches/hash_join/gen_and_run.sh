#!/bin/bash

# Exit immediately if a command exits with a non-zero status
set -e

# Get the absolute path of the directory containing the script
SCRIPT_DIR="$(cd "$(dirname "$0")"; pwd)"
echo "Script directory: $SCRIPT_DIR"

# Remove existing CSV files
echo "Deleting existing CSV files..."
rm -f "$SCRIPT_DIR/data.csv" "$SCRIPT_DIR/txs.csv" "$SCRIPT_DIR/ops.csv" "$SCRIPT_DIR/final_data.csv"

# Parameters for gen_data.py and gen_txs.py
# You can adjust these parameters as needed
# DATA_PARAMS="-n 100"
# TXS_PARAMS="-n 10 -minc 5 -maxc 10 -ro 0.5 -i 1.0 -u 0.0 -d 0.0"
DATA_PARAMS="-n 10"
TXS_PARAMS="-n 5 -minc 2 -maxc 4 -ro 0.5 -i 1.0 -u 0.0 -d 0.0"

# Run gen_data.py with parameters
echo "Running gen_data.py with parameters: $DATA_PARAMS"
python3 "$SCRIPT_DIR/gen_data.py" $DATA_PARAMS

# Check if data.csv was generated
if [ -f "$SCRIPT_DIR/data.csv" ]; then
    echo "data.csv generated successfully."
else
    echo "Error: data.csv not found. gen_data.py may have failed."
    exit 1
fi

# Run gen_txs.py with parameters
echo "Running gen_txs.py with parameters: $TXS_PARAMS"
python3 "$SCRIPT_DIR/gen_txs.py" $TXS_PARAMS

# Check if txs.csv and ops.csv were generated
if [ -f "$SCRIPT_DIR/txs.csv" ] && [ -f "$SCRIPT_DIR/ops.csv" ]; then
    echo "txs.csv and ops.csv generated successfully."
else
    echo "Error: txs.csv or ops.csv not found. gen_txs.py may have failed."
    exit 1
fi

echo "Data and transactions generated successfully."

# Run gen_final_data.py to generate final_data.csv
echo "Running gen_final_data.py..."
python3 "$SCRIPT_DIR/gen_data_after_txs.py"

# Check if final_data.csv was generated
if [ -f "$SCRIPT_DIR/data_after_txs.csv" ]; then
    echo "data_after_txs.csv generated successfully."
else
    echo "Error: data_after_txs.csv not found. gen_data_after_txs.py may have failed."
    exit 1
fi

echo "Final data generated successfully."

# Run the Rust benchmark
echo "Running Rust benchmark..."

# Assuming the Rust project root is two directories up from the script directory
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../../"; pwd)"
echo "Project root directory: $PROJECT_ROOT"

# Build absolute paths to data.csv and ops.csv
DATA_CSV="$SCRIPT_DIR/data.csv"
OPS_CSV="$SCRIPT_DIR/ops.csv"

# Change to the project root directory
cd "$PROJECT_ROOT"

# Run cargo from the project root, specifying the binary and the absolute paths to data.csv and ops.csv
cargo run --release --bin hash_join_bench -- "$DATA_CSV" "$OPS_CSV"

echo "Benchmark completed."
