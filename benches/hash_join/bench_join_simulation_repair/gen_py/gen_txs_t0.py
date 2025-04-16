#!/usr/bin/env python3

import os
import argparse
import csv
import random
import re
import string
import sys

# =============================================================================
# 1) Default Constants
# =============================================================================

DEFAULT_TABLE0 = "table_0.csv"
DEFAULT_TXS0_OUT = "txs_t0.csv"

DEFAULT_NUM_TRANSACTIONS = 10
DEFAULT_MIN_OPS = 3
DEFAULT_MAX_OPS = 3
DEFAULT_INSERT_RATIO = 0
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

def build_ops_dist_list(insert_r, get_r, update_r, delete_r):
    """
    Create a single flat list of operations based on their ratio.
    E.g. insert=2 => ['insert','insert']
    This way random.choice(...) is cheap.
    """
    ops_list = []
    ops_list += ["insert"] * insert_r
    ops_list += ["get"]    * get_r
    ops_list += ["update"] * update_r
    ops_list += ["delete"] * delete_r
    if not ops_list:  # fallback to something
        ops_list = ["update"]  # default
    return ops_list

# =============================================================================
# 3) The Improved TX Generation Function
# =============================================================================

def write_transactions_for_table_0(
    out_filename,
    table_dict,
    join_key_set,
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
    Creates a CSV file for table_0 transactions, with each TX bracketed by tx_begin/tx_commit.
    Weighted random among insert / get / update / delete.
    In each TX, a row can be modified only once (update or delete).
    
    To speed up:
      1) We keep a separate pkeys_list for random picks (rather than list(table_dict.keys())) each time.
      2) We pre-build ops_dist_list.
      3) We shuffle pkeys_list once per TX to pick unmodified pkeys in O(n) time without repeated random collisions.
      4) We accumulate rows in memory and write them at the end (fewer I/O calls).
    """
    fieldnames = ["tx_id", "ts", "op", "pkey", "join_key", "value"]
    # Pre-build the distribution of ops
    ops_dist_list = build_ops_dist_list(insert_ratio, get_ratio, update_ratio, delete_ratio)

    # Keep a list of pkeys for quick random selection (avoid building it repeatedly)
    pkeys_list = list(table_dict.keys())

    rows_to_write = []

    for tx_id in range(1, ntx + 1):
        ts = tx_id
        num_ops = random.randint(min_ops, max_ops)

        # For each TX, track which pkeys we have modified.
        modified_pkeys = set()

        # TX BEGIN
        rows_to_write.append({
            "tx_id": tx_id, "ts": ts,
            "op": "tx_begin",
            "pkey": "", "join_key": "", "value": ""
        })

        # We'll shuffle pkeys_list once for update/delete:
        random.shuffle(pkeys_list)
        pkey_idx = 0  # pointer in shuffled pkeys_list

        for _ in range(num_ops):
            op = random.choice(ops_dist_list)

            if op == "insert":
                # brand-new pkey
                new_pkey = generate_random_string(pkey_size)
                # ensure no collision with existing dictionary
                while new_pkey in table_dict:
                    new_pkey = generate_random_string(pkey_size)

                # pick an existing join key or create a new random one
                if join_key_set:
                    jk = random.choice(list(join_key_set))
                else:
                    jk = generate_random_string(8)

                val = generate_value_string(base_len_min, base_len_max, total_len_min, total_len_max)
                table_dict[new_pkey] = (jk, val)
                pkeys_list.append(new_pkey)  # track new pkey
                join_key_set.add(jk)

                rows_to_write.append({
                    "tx_id": tx_id, "ts": ts, "op": "insert",
                    "pkey": new_pkey, "join_key": jk, "value": val
                })

            elif op == "get":
                if not pkeys_list:
                    continue
                existing_pkey = random.choice(pkeys_list)
                jk, _old_val = table_dict[existing_pkey]
                rows_to_write.append({
                    "tx_id": tx_id, "ts": ts, "op": "get",
                    "pkey": existing_pkey, "join_key": jk, "value": ""
                })

            elif op in ("update", "delete"):
                if not pkeys_list:
                    continue

                # find next unmodified pkey from pkey_idx onward
                chosen_pkey = None
                while pkey_idx < len(pkeys_list):
                    candidate = pkeys_list[pkey_idx]
                    pkey_idx += 1
                    if candidate not in modified_pkeys:
                        chosen_pkey = candidate
                        break
                if not chosen_pkey:
                    # no unmodified pkeys left
                    continue
                modified_pkeys.add(chosen_pkey)

                jk, _old_val = table_dict[chosen_pkey]

                if op == "update":
                    new_val = generate_value_string(base_len_min, base_len_max, total_len_min, total_len_max)
                    table_dict[chosen_pkey] = (jk, new_val)
                    rows_to_write.append({
                        "tx_id": tx_id, "ts": ts, "op": "update",
                        "pkey": chosen_pkey, "join_key": jk, "value": new_val
                    })
                else:  # delete
                    del table_dict[chosen_pkey]
                    # remove from pkeys_list by "swapping with last" approach if we want O(1).
                    # But for simplicity, let's just do a remove() => O(n).
                    pkeys_list.remove(chosen_pkey)
                    rows_to_write.append({
                        "tx_id": tx_id, "ts": ts, "op": "delete",
                        "pkey": chosen_pkey, "join_key": jk, "value": ""
                    })

        # TX COMMIT
        rows_to_write.append({
            "tx_id": tx_id, "ts": ts,
            "op": "tx_commit",
            "pkey": "", "join_key": "", "value": ""
        })

    # Finally, write everything to disk in one pass
    with open(out_filename, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows_to_write)

# =============================================================================
# 4) Main
# =============================================================================

def main():
    parser = argparse.ArgumentParser(description="Generate TX logs (insert/get/update/delete) for table_0, improved version.")
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

    # Extract pkey_size from the first row's pkey length
    sample_pkey = next(iter(table_dict.keys()))
    pkey_size = len(sample_pkey)

    # Analyze table_0 values to preserve the same base-len/total-len distribution
    base_len_min, base_len_max, total_len_min, total_len_max = analyze_table_0_for_values(table_dict)
    print(f"Value lens in {args.table0}: base_len in [{base_len_min}..{base_len_max}], total_len in [{total_len_min}..{total_len_max}]")

    # Generate TX logs for table_0
    print(f"\nGenerating {args.num_transactions} transactions => {args.txs0_out}")
    write_transactions_for_table_0(
        out_filename=txs0_path,
        table_dict=table_dict,
        join_key_set=join_key_set,
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
