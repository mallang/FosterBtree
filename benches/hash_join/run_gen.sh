#!/bin/bash

# Exit immediately if a command exits with a non-zero status
set -e

# Get the absolute path of the directory containing the script
SCRIPT_DIR="$(cd "$(dirname "$0")"; pwd)"
echo "Script directory: $SCRIPT_DIR"

# Remove existing CSV files
echo "Deleting existing CSV files..."
rm -f "$SCRIPT_DIR/data.csv" "$SCRIPT_DIR/txs.csv" "$SCRIPT_DIR/ops.csv" \
      "$SCRIPT_DIR/data_after_txs.csv" \
      "$SCRIPT_DIR/recent_data_after_txs.csv" "$SCRIPT_DIR/history_data_after_txs.csv" \
      "$SCRIPT_DIR/recent_data_after_ops.csv" "$SCRIPT_DIR/history_data_after_ops.csv"
echo ""

# Parameters for gen_data.py and gen_txs.py
# You can adjust these parameters as needed

# Update TXS_PARAMS to include the get_ratio parameter
DATA_PARAMS="-n 1000 -k 8 -p 8 -vmin 32 -vmax 64 -s 5"
TXS_PARAMS="-n 100 -minc 8 -maxc 12 -ro 0.0 -i 0.1 -u 0.3 -d 0.1 -g 0.5"

# Run gen_data.py with parameters
echo "Running gen_data.py with parameters: $DATA_PARAMS"
python3 "$SCRIPT_DIR/gen_data.py" $DATA_PARAMS

# Check if data.csv was generated
if [ -f "$SCRIPT_DIR/data.csv" ]; then
    echo "data.csv generated successfully."
    echo ""
else
    echo "Error: data.csv not found. gen_data.py may have failed."
    exit 1
fi

# Run gen_txs.py (now generate_transactions.py) with parameters
echo "Running gen_txs.py with parameters: $TXS_PARAMS"
python3 "$SCRIPT_DIR/gen_txs.py" $TXS_PARAMS

# Check if txs.csv and ops.csv were generated
if [ -f "$SCRIPT_DIR/txs.csv" ] && [ -f "$SCRIPT_DIR/ops.csv" ]; then
    echo "txs.csv and ops.csv generated successfully."
    echo ""
else
    echo "Error: txs.csv or ops.csv not found. gen_txs.py may have failed."
    exit 1
fi

echo "Data and transactions generated successfully."
echo ""

# Run gen_data_after_txs.py to generate recent and history data after txs and ops
echo "Running gen_data_after_txs.py..."
python3 "$SCRIPT_DIR/gen_data_after_txs.py" \
    --data_file "$SCRIPT_DIR/data.csv" \
    --txs_file "$SCRIPT_DIR/txs.csv" \
    --ops_file "$SCRIPT_DIR/ops.csv" \
    --recent_data_after_txs_file "$SCRIPT_DIR/recent_data_after_txs.csv" \
    --history_data_after_txs_file "$SCRIPT_DIR/history_data_after_txs.csv" \
    --recent_data_after_ops_file "$SCRIPT_DIR/recent_data_after_ops.csv" \
    --history_data_after_ops_file "$SCRIPT_DIR/history_data_after_ops.csv"

# Generate scan operations
echo "Running gen_scan_ops.py..."
python3 "$SCRIPT_DIR/gen_scan_ops.py" \
    --recent_data_file "$SCRIPT_DIR/recent_data_after_txs.csv" \
    --history_data_file "$SCRIPT_DIR/history_data_after_txs.csv" \
    --scan_ops_file "$SCRIPT_DIR/scan_ops.csv" \
    --num_scans 5

# Check if recent and history data files were generated
if [ -f "$SCRIPT_DIR/recent_data_after_txs.csv" ] && [ -f "$SCRIPT_DIR/history_data_after_txs.csv" ] && \
   [ -f "$SCRIPT_DIR/recent_data_after_ops.csv" ] && [ -f "$SCRIPT_DIR/history_data_after_ops.csv" ]; then
    echo "Recent and history data after txs and ops generated successfully."
else
    echo "Error: Some data files were not found. gen_data_after_txs.py may have failed."
    exit 1
fi

echo ""
echo "Data generation completed."
