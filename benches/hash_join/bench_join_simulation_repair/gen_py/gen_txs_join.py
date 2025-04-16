#!/usr/bin/env python3
import os
import argparse
import csv
import sys
from math import ceil

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
CSV_DIR = os.path.join(SCRIPT_DIR, "..", "csv")

# Default: if no ts values are provided, default to [1]
DEFAULT_TS_VALUES = [1, 100]
# Default number of transactions per file.
DEFAULT_NUM_TX = 1

def distribute_rows_among_txs(rows, num_tx):
    """Partition 'rows' into num_tx groups (roughly equal)."""
    if num_tx <= 0:
        num_tx = 1
    total_rows = len(rows)
    group_size = ceil(total_rows / num_tx)
    row_groups = []
    idx = 0
    for _ in range(num_tx):
        chunk = rows[idx : idx + group_size]
        row_groups.append(chunk)
        idx += group_size
        if idx >= total_rows:
            break
    while len(row_groups) < num_tx:
        row_groups.append([])
    return row_groups

def write_join_scans_for_table(table_filename, out_filename, num_tx, ts):
    """
    Reads table_filename (which must have a 'join_key' column),
    partitions its rows among num_tx transactions,
    and writes a TX log with columns [tx_id, ts, op, join_key].
    The starting transaction id is 1
    """
    rows = []
    with open(table_filename, "r", newline="") as f:
        reader = csv.DictReader(f)
        if "join_key" not in reader.fieldnames:
            raise ValueError(f"{table_filename} must have a 'join_key' column.")
        for row in reader:
            rows.append(row["join_key"])

    groups = distribute_rows_among_txs(rows, num_tx)
    fieldnames = ["tx_id", "ts", "op", "join_key"]
    tx_id = 1
    with open(out_filename, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for group in groups:
            writer.writerow({
                "tx_id": tx_id, "ts": ts, "op": "tx_begin", "join_key": ""
            })
            for jk in group:
                writer.writerow({
                    "tx_id": tx_id, "ts": ts, "op": "scan_with_join_key", "join_key": jk
                })
            writer.writerow({
                "tx_id": tx_id, "ts": ts, "op": "tx_commit", "join_key": ""
            })
            tx_id += 1
    
def main():
    parser = argparse.ArgumentParser(
        description="Generate join logs (scan_with_join_key) for join tables (table_1.csv, table_2.csv, etc.). "
                    "For each discovered join table and for each ts value provided, a separate CSV file is generated."
    )
    parser.add_argument(
        "--ts-values",
        type=int,
        nargs="+",
        default=DEFAULT_TS_VALUES,
        help=("List of starting ts values for join logs. For each discovered join table, a file will be generated "
              "for each ts value. (default: %(default)s)")
    )
    parser.add_argument(
        "--num_tx",
        type=int,
        default=DEFAULT_NUM_TX,
        help="Number of transactions per output file (default: %(default)s)."
    )
    args = parser.parse_args()

    if not os.path.exists(CSV_DIR):
        print(f"[ERROR] CSV directory not found: {CSV_DIR}")
        sys.exit(1)

    # Discover join tables: table_1.csv, table_2.csv, ... in CSV_DIR
    discovered = []
    i = 1
    while True:
        table_fname = f"table_{i}.csv"
        table_path = os.path.join(CSV_DIR, table_fname)
        if not os.path.exists(table_path):
            break
        discovered.append(table_fname)
        i += 1

    if not discovered:
        print("No join tables discovered (table_1.csv, table_2.csv, etc.). Nothing to do.")
        sys.exit(0)

    print(f"Discovered {len(discovered)} join table(s): {discovered}")
    print(f"Using ts values: {args.ts_values}")
    print(f"Number of transactions per file: {args.num_tx}")

    # For each discovered join table and for each ts value, generate an output file.
    for idx, table_fname in enumerate(discovered, start=1):
        table_path = os.path.join(CSV_DIR, table_fname)
        for ts_val in args.ts_values:
            # Build output file name: "txs_join_ts{ts}_t{idx}.csv"
            out_fname = f"txs_join_ts{ts_val}_t{idx}.csv"
            out_path = os.path.join(CSV_DIR, out_fname)
            print(f"Generating join log for {table_fname} with starting ts {ts_val} -> {out_fname}")
            write_join_scans_for_table(
                table_filename=table_path,
                out_filename=out_path,
                num_tx=args.num_tx,
                ts=ts_val
            )
            print(f"  -> Wrote {out_fname} with {args.num_tx} TX(s) at ts {ts_val}")

    print("\nAll done! Created join logs for discovered join tables.")


if __name__ == "__main__":
    main()
