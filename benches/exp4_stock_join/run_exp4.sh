#!/bin/bash
# Experiment 4: ORDER_LINE ⋈ STOCK (HTAP Join Validation)
# Runs J-U-J benchmark for all configurations × update intensities (Zipfian).
#
# Usage: ./run_exp4.sh [WAREHOUSES] [OUTPUT_CSV]
# Example:
#   ./run_exp4.sh           # W=1, output to data/exp4_results.csv
#   ./run_exp4.sh 1 data/exp4_results.csv

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
DATA_DIR="$SCRIPT_DIR/data"
FIGS_DIR="$SCRIPT_DIR/figs"

W="${1:-1}"
OUT_CSV="${2:-$DATA_DIR/exp4_results.csv}"
DIST="zipf0.99"
ZIPF_THETA="0.99"
BUCKET_NUM=1024
WARMUP=1
REPEAT=10
TRIM=2

mkdir -p "$DATA_DIR" "$FIGS_DIR"

# ── 1. Build binary ──────────────────────────────────────────────────────────
echo "=== Building stock_join_bench ==="
cd "$ROOT"
cargo build --release --bin stock_join_bench
BIN="$ROOT/target/release/stock_join_bench"
echo "Build OK: $BIN"

# ── 2. Generate CH-benCHmark data (if not present) ──────────────────────────
STOCK_FILE="$DATA_DIR/stock_w${W}.tbl"
ORDERLINE_FILE="$DATA_DIR/orderline_w${W}.tbl"

if [ ! -f "$STOCK_FILE" ] || [ ! -f "$ORDERLINE_FILE" ]; then
    echo "=== Generating CH-benCHmark data (W=$W) ==="
    python3 "$SCRIPT_DIR/generate_chbenchmark_data.py" \
        --warehouses "$W" \
        --output-dir "$DATA_DIR"
else
    echo "Data files already exist (W=$W). Skipping generation."
fi

STOCK_ROWS=$(wc -l < "$STOCK_FILE")
OL_ROWS=$(wc -l < "$ORDERLINE_FILE")
echo "  STOCK:      $STOCK_ROWS rows"
echo "  ORDER_LINE: $OL_ROWS rows"

# ── 3. Generate update files for all intensities ─────────────────────────────
echo "=== Generating update files ==="
UPDATE_PCTS=(0 5 10 15 20)

for PCT in "${UPDATE_PCTS[@]}"; do
    UPDATES_FILE="$DATA_DIR/stock_updates_w${W}_${PCT}pct_${DIST}.tbl"
    if [ -f "$UPDATES_FILE" ]; then
        echo "  ${PCT}% update file exists, skipping."
        continue
    fi
    if [ "$PCT" -eq 0 ]; then
        # 0% = empty file (no updates)
        touch "$UPDATES_FILE"
        echo "  0% -> empty file created."
    else
        python3 "$SCRIPT_DIR/generate_stock_updates.py" \
            "$STOCK_FILE" "$PCT" "$W" \
            --zipf "$ZIPF_THETA" \
            --output-dir "$DATA_DIR"
        echo "  ${PCT}% -> $UPDATES_FILE"
    fi
done

# ── 4. Run benchmark ──────────────────────────────────────────────────────────
echo "=== Running Experiment 4 ==="

# All configurations: (table_type, repair_mode, label)
CONFIGS=(
    "naive nr SNAP"
    "heap  nr MONO-NR"
    "heap  rr MONO-RR"
    "heap  wr MONO-WR"
    "chain wr DUAL-WR"
    "par   nr EPOCH-NR"
    "par   rr EPOCH-RR"
    "par   wr EPOCH-WR"
)

# Remove old CSV to start fresh
if [ -f "$OUT_CSV" ]; then
    rm "$OUT_CSV"
    echo "Removed old CSV: $OUT_CSV"
fi

TOTAL_RUNS=$(( ${#CONFIGS[@]} * ${#UPDATE_PCTS[@]} ))
CURRENT=0

for PCT in "${UPDATE_PCTS[@]}"; do
    UPDATES_FILE="$DATA_DIR/stock_updates_w${W}_${PCT}pct_${DIST}.tbl"

    for CFG in "${CONFIGS[@]}"; do
        read -r TABLE_TYPE REPAIR_MODE LABEL <<< "$CFG"
        CURRENT=$(( CURRENT + 1 ))
        echo ""
        echo "[$CURRENT/$TOTAL_RUNS] ${LABEL} | update=${PCT}% | ${DIST}"

        "$BIN" \
            --stock-file       "$STOCK_FILE" \
            --orderline-file   "$ORDERLINE_FILE" \
            --updates-file     "$UPDATES_FILE" \
            --table-type       "$TABLE_TYPE" \
            --repair-mode      "$REPAIR_MODE" \
            --bucket-num       "$BUCKET_NUM" \
            --warmup           "$WARMUP" \
            --repeat           "$REPEAT" \
            --trim             "$TRIM" \
            --update-pct       "$PCT" \
            --distribution     "$DIST" \
            --output-csv       "$OUT_CSV"
    done
done

echo ""
echo "=== Experiment 4 complete ==="
echo "Results: $OUT_CSV"
echo ""
echo "To generate plots, open exp4_plot.ipynb or run:"
echo "  jupyter nbconvert --to notebook --execute exp4_plot.ipynb"
