#!/usr/bin/env python3

import os
import argparse
import csv
import sys
from math import ceil

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
CSV_DIR = os.path.join(SCRIPT_DIR, "..", "csv")

DEFAULT_TX_JOIN_TABLES = [1]

def distribute_rows_among_txs(rows, num_txs):
    """Partition 'rows' into num_txs groups (roughly equal). Each group => one TX."""
    if num_txs <= 0:
        num_txs = 1
    total_rows = len(rows)
    group_size = ceil(total_rows / num_txs)
    row_groups = []
    idx = 0
    for _ in range(num_txs):
        chunk = rows[idx : idx + group_size]
        row_groups.append(chunk)
        idx += group_size
        if idx >= total_rows:
            break
    while len(row_groups) < num_txs:
        row_groups.append([])
    return row_groups

def write_join_scans_for_table(table_filename, out_filename, num_txs, start_tx_id=1):
    """
    Reads table_filename (must have a 'join_key' column),
    splits into num_txs transactions, each row => 'scan_with_join_key'.
    Writes to out_filename => columns [tx_id, ts, op, join_key].
    Returns last tx_id used so we can continue numbering if needed.
    """
    rows = []
    with open(table_filename, "r", newline="") as f:
        reader = csv.DictReader(f)
        if "join_key" not in reader.fieldnames:
            raise ValueError(f"{table_filename} must have 'join_key' column.")
        for row in reader:
            rows.append(row["join_key"])

    groups = distribute_rows_among_txs(rows, num_txs)

    fieldnames = ["tx_id", "ts", "op", "join_key"]
    with open(out_filename, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        tx_id = start_tx_id
        for group in groups:
            ts = tx_id
            # TX BEGIN
            writer.writerow({
                "tx_id": tx_id, "ts": ts,
                "op": "tx_begin",
                "join_key": ""
            })
            for jk in group:
                writer.writerow({
                    "tx_id": tx_id,
                    "ts": ts,
                    "op": "scan_with_join_key",
                    "join_key": jk
                })
            # TX COMMIT
            writer.writerow({
                "tx_id": tx_id, "ts": ts,
                "op": "tx_commit",
                "join_key": ""
            })
            tx_id += 1
    return tx_id - 1

def main():
    parser = argparse.ArgumentParser(description="Generate join logs (scan_with_join_key) for table_1..n => txs_join_t{i}.csv.")
    parser.add_argument(
        "-tjt", "--txs_join_tables",
        type=int,
        nargs="+",
        default=DEFAULT_TX_JOIN_TABLES,
        help=("Number of transactions for each discovered join table (table_1.csv, table_2.csv, etc.). "
              "If one value is provided, it applies to all discovered tables. Otherwise, specify multiple values. "
              f"(default: {DEFAULT_TX_JOIN_TABLES})")
    )
    args = parser.parse_args()

    # Ensure CSV_DIR
    if not os.path.exists(CSV_DIR):
        print(f"[ERROR] CSV directory not found: {CSV_DIR}")
        sys.exit(1)

    # Discover table_1.csv, table_2.csv, ...
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

    print(f"Discovered {len(discovered)} table(s): {discovered}")

    # If user provides a single -tjt => apply to all discovered tables
    # Or if the user provides exactly as many as discovered => map them
    if len(args.txs_join_tables) == 1:
        tx_counts = [args.txs_join_tables[0]] * len(discovered)
    elif len(args.txs_join_tables) == len(discovered):
        tx_counts = args.txs_join_tables
    else:
        print("[ERROR] -tjt must have either 1 value or match the number of discovered tables.")
        sys.exit(1)

    current_tx_id = 1
    for idx, tab in enumerate(discovered, start=1):
        tpath = os.path.join(CSV_DIR, tab)
        out_join_fname = f"txs_join_t{idx}.csv"
        out_join_path = os.path.join(CSV_DIR, out_join_fname)
        tx_for_table = tx_counts[idx - 1]

        print(f"Generating join logs for {tab} => {out_join_fname} with {tx_for_table} TX(s).")
        last_tx_id = write_join_scans_for_table(
            table_filename=tpath,
            out_filename=out_join_path,
            num_txs=tx_for_table,
            start_tx_id=current_tx_id
        )
        used = last_tx_id - current_tx_id + 1
        print(f" => Wrote {out_join_fname} with {used} TX(s).")
        current_tx_id = last_tx_id + 1

    print("\nAll done! Created join logs for discovered tables.")


if __name__ == "__main__":
    main()
