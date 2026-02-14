#!/bin/bash
# Generate TPC-H PART & LINEITEM tables and update files.
# Usage: ./generate_data.sh [SF] [UPDATE_PCT]
# Example: ./generate_data.sh 0.1 1

set -e

SF="${1:-0.1}"
UPDATE_PCT="${2:-1}"

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
DBGEN_DIR="$SCRIPT_DIR/dbgen"
OUT_DIR="$SCRIPT_DIR/tpch_data"

mkdir -p "$OUT_DIR"

echo "=== Building dbgen ==="
cd "$DBGEN_DIR"
if [ ! -f dbgen ]; then
    make
fi

echo "=== Generating TPC-H tables (SF=$SF) ==="
./dbgen -s "$SF" -T P -f
./dbgen -s "$SF" -T L -f
mv part.tbl "$OUT_DIR/part_sf${SF}.tbl"
mv lineitem.tbl "$OUT_DIR/lineitem_sf${SF}.tbl"

echo "=== Generating updates (${UPDATE_PCT}% uniform) ==="
cd "$SCRIPT_DIR"
python3 generate_updates.py "$OUT_DIR/part_sf${SF}.tbl" "$UPDATE_PCT" "$SF"

echo "=== Generating updates (${UPDATE_PCT}% zipf0.99) ==="
python3 generate_updates.py "$OUT_DIR/part_sf${SF}.tbl" "$UPDATE_PCT" "$SF" --zipf 0.99

echo "=== Done ==="
ls -lh "$OUT_DIR"/*sf${SF}*
