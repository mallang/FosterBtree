#!/usr/bin/env bash
# E1 at real scale (Step 2 of the restart plan).
#
# Three PagedHashChainV1 regimes chosen from the 16KB-page math
# (~441 records/page for 8B keys, meta-page cap ~4091 buckets):
#
#   A  n_keys=1M,  buckets=4000  -> ~250 keys/bucket, chain ~1 page   (low)
#   B  n_keys=10M, buckets=4000  -> ~2500/bucket,     chain ~6 pages  (mid)
#   C  n_keys=10M, buckets=1024  -> ~9766/bucket,     chain ~23 pages (heavy)
#
# hash_btree is bucket-independent: run once per n_keys.
# Uniform distribution, 1 thread, 1M measured lookups per run.
#
# Expected wall time on an M-series Mac (release build):
#   compile+test ~2-5 min, A ~1 min, B ~5 min, C ~10-15 min (build dominates).
#
# Usage:  ./scripts/run_e1_scale.sh
# Output: results/hash_vs_tree_v0/e1_scale_<timestamp>.csv

set -euo pipefail
cd "$(dirname "$0")/.."

STAMP=$(date +%Y%m%d_%H%M%S)
OUT_DIR=results/hash_vs_tree_v0
OUT=$OUT_DIR/e1_scale_$STAMP.csv
mkdir -p "$OUT_DIR"

CARGO_FLAGS=(--release --no-default-features)
BIN=(cargo run "${CARGO_FLAGS[@]}" --bin hash_vs_tree_v0 --)

echo "== 1/3 compile + unit tests =="
cargo test "${CARGO_FLAGS[@]}" --lib -- paged_hash_chain_v1 hash_leaf
cargo build "${CARGO_FLAGS[@]}" --bin hash_vs_tree_v0

echo "== 2/3 smoke (5K keys, all variants, verify new CSV columns) =="
"${BIN[@]}" --variant all --n-keys 5000 --lookups-per-thread 5000 \
  --warmup-lookups-per-thread 500 --threads 2 --distribution uniform \
  --buckets 512 --value-size 8 --key-size 8

echo "== 3/3 E1 scale runs -> $OUT =="
run() { # run <extra args...>; append rows, keep only first header
  if [[ -s "$OUT" ]]; then
    "${BIN[@]}" "$@" | tail -n +2 >> "$OUT"
  else
    "${BIN[@]}" "$@" >> "$OUT"
  fi
}

COMMON=(--distribution uniform --threads 1 \
        --lookups-per-thread 1000000 --warmup-lookups-per-thread 100000 \
        --value-size 8 --key-size 8)

# --- Config A: low overflow (chain ~1) ---
run --variant paged-hash-chain-v1 --n-keys 1000000  --buckets 4000 "${COMMON[@]}"
run --variant hash-btree          --n-keys 1000000  --buckets 4000 "${COMMON[@]}"

# --- Config B: mid overflow (chain ~6) ---
run --variant paged-hash-chain-v1 --n-keys 10000000 --buckets 4000 "${COMMON[@]}"
run --variant hash-btree          --n-keys 10000000 --buckets 4000 "${COMMON[@]}"

# --- Config C: heavy overflow (chain ~23); tree row identical to B's, skip ---
run --variant paged-hash-chain-v1 --n-keys 10000000 --buckets 1024 "${COMMON[@]}"

echo
echo "== done =="
column -s, -t "$OUT" | cut -c1-200
echo
echo "CSV: $OUT"
echo "Check: avg_chain_len ~1 (A) / ~6 (B) / ~23 (C); tree_height filled for hash_btree rows."
