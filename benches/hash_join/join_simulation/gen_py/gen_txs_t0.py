#!/usr/bin/env python3

import os
import argparse
import csv
import random
import re
import string
import sys
from math import ceil

# =============================================================================
# 1) Default Constants
# =============================================================================

DEFAULT_TABLE0 = "table_0.csv"
DEFAULT_TXS0_OUT = "txs_t0.csv"

DEFAULT_NUM_TRANSACTIONS = 10
DEFAULT_MIN_OPS = 10
DEFAULT_MAX_OPS = 10
DEFAULT_INSERT_RATIO = 1
DEFAULT_GET_RATIO = 0
DEFAULT_UPDATE_RATIO = 1
DEFAULT_DELETE_RATIO = 0

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
CSV_DIR = os.path.join(SCRIPT_DIR, "..", "csv")

# =============================================================================
# 2) Helpers
# =============================================================================

def parse_value_field(value_str):
    """
    Expects 'BASESTRING(TOTAL_LENGTH)' format, e.g. 'abcXYZ123(800)'.
    Returns (base_string, total_length) or (None, None) if unmatched.
    """
    pattern = r"^(.*)\((\d+)\)$"
    match = re.match(pattern, value_str)
    if match:
        base_part = match.group(1)
        total_len = int(match.group(2))
        return base_part, total_len
    return None, None

def analyze_table_0_for_values(table_0_dict):
    """
    table_0_dict = {pkey: (join_key, value_str)}
    Returns (base_len_min, base_len_max, total_len_min, total_len_max).
    Raises ValueError if no valid value fields found.
    """
    base_lens = []
    total_lens = []
    for _, (_, val_str) in table_0_dict.items():
        base_part, t_len = parse_value_field(val_str)
        if base_part and t_len is not None:
            base_lens.append(len(base_part))
            total_lens.append(t_len)
    if not base_lens or not total_lens:
        raise ValueError("No valid 'value' fields in table_0 with 'BASESTRING(TOTAL_LENGTH)' format.")
    return min(base_lens), max(base_lens), min(total_lens), max(total_lens)

def read_table_0(filepath):
    """
    Reads CSV at 'filepath' -> returns (table_dict, join_key_set).
    table_dict = { pkey: (join_key, value_str) }
    join_key_set = set of all join_keys
    """
    table_dict = {}
    join_key_set = set()
    with open(filepath, "r", newline="") as f:
        reader = csv.DictReader(f)
        required = {"pkey", "join_key", "value"}
        if not required.issubset(reader.fieldnames):
            raise ValueError(f"{filepath} must have columns {', '.join(required)}.")
        for row in reader:
            pkey = row["pkey"]
            jkey = row["join_key"]
            val = row["value"]
            table_dict[pkey] = (jkey, val)
            join_key_set.add(jkey)
    return table_dict, join_key_set

def pick_operation(ops_distribution):
    """
    Weighted random choice among the distribution, e.g.:
      {"insert":2, "get":1, "update":1, "delete":1}
    """
    weighted_ops = []
    for op_name, count in ops_distribution.items():
        weighted_ops.extend([op_name] * count)
    return random.choice(weighted_ops)

def generate_random_string(length=8):
    """Generate random alphanumeric string of given length."""
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))

def generate_value_string(base_len_min, base_len_max, total_len_min, total_len_max):
    """
    1) Choose random base_len in [base_len_min..base_len_max].
    2) Generate that base string.
    3) Choose random total_len in [total_len_min..total_len_max].
    4) Return 'BASESTRING(TOTAL_LENGTH)'.
    """
    chosen_base_len = random.randint(base_len_min, base_len_max)
    base_str = generate_random_string(chosen_base_len)
    chosen_total_len = random.randint(total_len_min, total_len_max)
    return f"{base_str}({chosen_total_len})"

def write_transactions_for_table_0(
    out_filename,
    table_dict,
    all_join_keys,
    ntx,            # number of transactions
    min_ops,
    max_ops,
    insert_ratio,
    get_ratio,
    update_ratio,
    delete_ratio,
    pkey_size,
    base_len_min,
    base_len_max,
    total_len_min,
    total_len_max
):
    """
    Creates a CSV file for table_0 transactions, with each TX bracketed by tx_begin and tx_commit.
    Weighted random among insert / get / update / delete.
    In each TX, a row can be modified only once (update or delete).
    """
    fieldnames = ["tx_id", "ts", "op", "pkey", "join_key", "value"]
    ops_dist = {
        "insert": insert_ratio,
        "get": get_ratio,
        "update": update_ratio,
        "delete": delete_ratio
    }
    
    with open(out_filename, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        for tx_id in range(1, ntx + 1):
            ts = tx_id
            num_ops = random.randint(min_ops, max_ops)

            # Track which pkeys we've modified in this TX to avoid multiple modifications of the same row
            modified_pkeys = set()

            # TX BEGIN
            writer.writerow({
                "tx_id": tx_id, "ts": ts,
                "op": "tx_begin",
                "pkey": "", "join_key": "", "value": ""
            })

            for _ in range(num_ops):
                op = pick_operation(ops_dist)

                if op == "insert":
                    # brand-new pkey
                    new_pkey = generate_random_string(pkey_size)
                    # ensure no collision with existing dictionary
                    while new_pkey in table_dict:
                        new_pkey = generate_random_string(pkey_size)

                    # pick an existing join key or create a new random one
                    if all_join_keys:
                        jk = random.choice(list(all_join_keys))
                    else:
                        jk = generate_random_string(8)

                    val = generate_value_string(base_len_min, base_len_max, total_len_min, total_len_max)
                    table_dict[new_pkey] = (jk, val)
                    all_join_keys.add(jk)

                    writer.writerow({
                        "tx_id": tx_id, "ts": ts, "op": "insert",
                        "pkey": new_pkey, "join_key": jk, "value": val
                    })

                elif op == "get":
                    # read ops do not count as "modify", so no restriction
                    if not table_dict:
                        continue
                    existing_pkey = random.choice(list(table_dict.keys()))
                    jk, _old_val = table_dict[existing_pkey]
                    writer.writerow({
                        "tx_id": tx_id, "ts": ts, "op": "get",
                        "pkey": existing_pkey, "join_key": jk, "value": ""
                    })

                elif op == "update":
                    # pick random row that we have not modified yet in this TX
                    if not table_dict:
                        continue
                    # we attempt to find a distinct pkey
                    attempts = 0
                    max_attempts = 1000  # break if table is huge or random distribution is tough
                    existing_pkey = None
                    while attempts < max_attempts:
                        candidate = random.choice(list(table_dict.keys()))
                        if candidate not in modified_pkeys:
                            existing_pkey = candidate
                            break
                        attempts += 1
                    if existing_pkey is None:
                        # no unmodified row found
                        continue
                    modified_pkeys.add(existing_pkey)

                    # do the update
                    jk, _old_val = table_dict[existing_pkey]
                    new_val = generate_value_string(base_len_min, base_len_max, total_len_min, total_len_max)
                    table_dict[existing_pkey] = (jk, new_val)

                    writer.writerow({
                        "tx_id": tx_id, "ts": ts, "op": "update",
                        "pkey": existing_pkey, "join_key": jk, "value": new_val
                    })

                elif op == "delete":
                    # pick random row that we have not modified yet in this TX
                    if not table_dict:
                        continue
                    attempts = 0
                    max_attempts = 1000
                    existing_pkey = None
                    while attempts < max_attempts:
                        candidate = random.choice(list(table_dict.keys()))
                        if candidate not in modified_pkeys:
                            existing_pkey = candidate
                            break
                        attempts += 1
                    if existing_pkey is None:
                        # no unmodified row found
                        continue
                    modified_pkeys.add(existing_pkey)

                    # do the delete
                    jk, _val = table_dict[existing_pkey]
                    del table_dict[existing_pkey]

                    writer.writerow({
                        "tx_id": tx_id, "ts": ts, "op": "delete",
                        "pkey": existing_pkey, "join_key": jk, "value": ""
                    })

            # TX COMMIT
            writer.writerow({
                "tx_id": tx_id, "ts": ts,
                "op": "tx_commit",
                "pkey": "", "join_key": "", "value": ""
            })

# =============================================================================
# 3) Main
# =============================================================================

def main():
    parser = argparse.ArgumentParser(description="Generate TX logs (insert/get/update/delete) for table_0.")
    parser.add_argument("--table0", default=DEFAULT_TABLE0,
                        help=f"Base table filename (default: {DEFAULT_TABLE0}).")
    parser.add_argument("--txs0-out", default=DEFAULT_TXS0_OUT,
                        help=f"Output TX CSV for table_0 (default: {DEFAULT_TXS0_OUT}).")
    parser.add_argument("-ntx", "--num_transactions", type=int,
                        default=DEFAULT_NUM_TRANSACTIONS,
                        help=f"Number of transactions for table_0 (default: {DEFAULT_NUM_TRANSACTIONS}).")
    parser.add_argument("-minop", "--min_ops_per_tx", type=int,
                        default=DEFAULT_MIN_OPS,
                        help=f"Minimum ops per transaction (default: {DEFAULT_MIN_OPS}).")
    parser.add_argument("-maxop", "--max_ops_per_tx", type=int,
                        default=DEFAULT_MAX_OPS,
                        help=f"Maximum ops per transaction (default: {DEFAULT_MAX_OPS}).")
    parser.add_argument("-ir", "--insert_ratio", type=int,
                        default=DEFAULT_INSERT_RATIO,
                        help=f"Weight for inserts (default: {DEFAULT_INSERT_RATIO}).")
    parser.add_argument("-gr", "--get_ratio", type=int,
                        default=DEFAULT_GET_RATIO,
                        help=f"Weight for gets (default: {DEFAULT_GET_RATIO}).")
    parser.add_argument("-ur", "--update_ratio", type=int,
                        default=DEFAULT_UPDATE_RATIO,
                        help=f"Weight for updates (default: {DEFAULT_UPDATE_RATIO}).")
    parser.add_argument("-dr", "--delete_ratio", type=int,
                        default=DEFAULT_DELETE_RATIO,
                        help=f"Weight for deletes (default: {DEFAULT_DELETE_RATIO}).")

    args = parser.parse_args()

    # Ensure CSV_DIR
    if not os.path.exists(CSV_DIR):
        os.makedirs(CSV_DIR, exist_ok=True)
        print(f"[INFO] Created CSV directory: {CSV_DIR}")

    table0_path = os.path.join(CSV_DIR, args.table0)
    txs0_path = os.path.join(CSV_DIR, args.txs0_out)

    print(f"Reading table_0 from {table0_path}")
    if not os.path.exists(table0_path):
        print(f"[ERROR] {table0_path} not found.")
        sys.exit(1)

    table_dict, join_key_set = read_table_0(table0_path)
    if not table_dict:
        print(f"[ERROR] {table0_path} is empty or missing required columns.")
        sys.exit(1)

    # Extract pkey_size from first row
    sample_pkey = next(iter(table_dict.keys()))
    pkey_size = len(sample_pkey)

    # Analyze table_0 values
    base_len_min, base_len_max, total_len_min, total_len_max = analyze_table_0_for_values(table_dict)
    print(f"Value lengths in {args.table0}: base_len in [{base_len_min}..{base_len_max}], total_len in [{total_len_min}..{total_len_max}]")

    # Generate TX logs for table_0
    print(f"\nGenerating {args.num_transactions} transactions => {args.txs0_out}")
    write_transactions_for_table_0(
        out_filename=txs0_path,
        table_dict=table_dict,
        all_join_keys=join_key_set,
        ntx=args.num_transactions,
        min_ops=args.min_ops_per_tx,
        max_ops=args.max_ops_per_tx,
        insert_ratio=args.insert_ratio,
        get_ratio=args.get_ratio,
        update_ratio=args.update_ratio,
        delete_ratio=args.delete_ratio,
        pkey_size=pkey_size,
        base_len_min=base_len_min,
        base_len_max=base_len_max,
        total_len_min=total_len_min,
        total_len_max=total_len_max
    )
    print(f" => Wrote table_0 TX log: {args.txs0_out}\nAll done!")


if __name__ == "__main__":
    main()
