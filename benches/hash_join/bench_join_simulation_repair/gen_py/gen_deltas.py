import os
import argparse
import csv
import random
import string
import sys

DEFAULT_NUM_TABLES = 2
DEFAULT_NUM_ROWS = 10
DEFAULT_PRIMARY_KEY_SIZE = 8        # Length of table primary key
DEFAULT_JOIN_KEY_POOL_SIZE = 2    # Number of distinct keys in the pool
DEFAULT_JOIN_KEY_SIZE = 8           # Length of each key in the pool

FIXED_VALUE_BASE_SIZE = 16          # Fixed base string length for value column

# Example: a function to compute min/max value size
def compute_value_size():
    return 1000 - DEFAULT_PRIMARY_KEY_SIZE - DEFAULT_JOIN_KEY_SIZE

DEFAULT_MIN_VALUE_SIZE = compute_value_size()
DEFAULT_MAX_VALUE_SIZE = DEFAULT_MIN_VALUE_SIZE

# Determine script directory and csv folder
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
CSV_DIR = os.path.join(SCRIPT_DIR, '..', 'csv')

# =============================================================================
# 2) Helper Functions
# =============================================================================

def generate_unique_keys(num_keys, key_length):
    """
    Generate a set of unique alphanumeric keys of a given length.
    """
    keys = set()
    while len(keys) < num_keys:
        rand_key = ''.join(random.choices(string.ascii_letters + string.digits, k=key_length))
        keys.add(rand_key)
    return list(keys)
def generate_base_value():
    """
    Generate a fixed-length random base string of size FIXED_VALUE_BASE_SIZE.
    """
    return ''.join(random.choices(string.ascii_letters + string.digits, k=FIXED_VALUE_BASE_SIZE))


# =============================================================================
# 3) generate deltas
# =============================================================================
import random
def insert_ordered_elements_in_place(target, elements):
    n = len(target)
    if n < len(elements):
        for e in elements:
            target.append(e)
        return
    insert_positions = sorted(random.sample(range(n + 1), len(elements)))
    # Insert from last to first to avoid shifting problems
    for pos, val in reversed(list(zip(insert_positions, elements))):
        target.insert(pos, val)

def split_list_into_partitions(lst, num_parts):
    if num_parts <= 0:
        raise ValueError("num_parts must be greater than 0")
    
    base_size = len(lst) // num_parts
    remainder = len(lst) % num_parts

    partitions = []
    start = 0
    for i in range(num_parts):
        end = start + base_size
        # put all the extra (remainder) elements into the last partition
        if i == num_parts - 1:
            end += remainder
        partitions.append(lst[start:end])
        start = end

    return partitions

def gen_val(base_value, min_value_size, max_value_size):
    val_length = random.randint(min_value_size, max_value_size)
    value_data = f"{base_value}({val_length})"
    return value_data

def gen_deltas_of_table(num_versions, row_counts, join_key_pool, primary_key_size, min_value_size, max_value_size,
               num_deltas):
    table_filename = os.path.join(CSV_DIR, f"table_deltas.csv")
    print(f"Generating {table_filename} with {row_counts} rows each of which has {num_versions} versions...")
    
    total_versions = []
    for row_idx in range(row_counts):
        pkey = ''.join(random.choices(string.ascii_letters + string.digits, k=primary_key_size))
        join_key = random.choice(join_key_pool) # join_key: randomly selected from the pool
        base_value = generate_base_value()
        
        # 1# of insert key, pkey, value
        # version - 1# of update, key, pkey, value
        ins_and_upd = []
        ins_and_upd.append(['insert', pkey, join_key, gen_val(base_value, min_value_size, max_value_size)])
        for _ in range(1, num_versions):
            ins_and_upd.append(['update', pkey, join_key, gen_val(base_value, min_value_size, max_value_size)])

        insert_ordered_elements_in_place(total_versions, ins_and_upd)
    delta_versions_list = split_list_into_partitions(total_versions, num_deltas)

    
    uuid = 0
    with open(table_filename, "w", newline="") as tf:
        writer = csv.writer(tf)
        writer.writerow(['ts', 'tx_id', 'op', 'pkey', 'key', 'value'])  # Column headers
        for idx, delta in enumerate(delta_versions_list):
            if idx == 0:
                for op in delta:
                    writer.writerow([uuid, uuid, op[0], op[1], op[2], op[3]])  # Column headers
                uuid += 1
            else:
                for op in delta:
                    writer.writerow([uuid, uuid, op[0], op[1], op[2], op[3]])  # Column headers
                    uuid += 1
            writer.writerow([uuid, uuid, 'split_delta', '', '', ''])  # Column headers
            uuid += 1

    print(f"  -> {table_filename} generated.")
    return uuid

# =============================================================================
# 3) generate scan keys
# =============================================================================
def gen_scan_keys(number, join_key_pool, ts_list):
    num_rows = number
    ts1 = ts_list[0]
    ts2 = ts_list[1]
    table_filename1 = os.path.join(CSV_DIR, f"join_keys_history.csv")
    table_filename2 = os.path.join(CSV_DIR, f"join_keys_recent.csv")
    with open(table_filename1, "w", newline="") as tf1:
        with open(table_filename2, "w", newline="") as tf2:
            writer1 = csv.writer(tf1)
            writer2 = csv.writer(tf2)
            print(f"Generating {table_filename1}, {table_filename2} with {num_rows} rows...")
            writer1.writerow(["join_key", "ts"])
            writer2.writerow(["join_key", "ts"])
            for _ in range(num_rows):
                # join_key: randomly selected from the pool
                join_key = random.choice(join_key_pool)
                writer1.writerow([join_key, ts1])
                writer2.writerow([join_key, ts2])

    print(f"  -> {table_filename1}, {table_filename2} generated.")

def main():
    parser = argparse.ArgumentParser(description="Generate key pool and multiple tables for hash join simulation.")
    parser.add_argument(
        "-pknum", "--primary_key_num",
        type=int,
        default=10,
        help=f"Number of primary keys in table (default: {10})."
    )
    parser.add_argument(
        "-jkps", "--join_key_pool_size",
        type=int,
        default=DEFAULT_JOIN_KEY_POOL_SIZE,
        help=f"Number of distinct join keys to include in the key pool (default: {DEFAULT_JOIN_KEY_POOL_SIZE})."
    )
    parser.add_argument(
        "-jks", "--join_key_size",
        type=int,
        default=DEFAULT_JOIN_KEY_SIZE,
        help=f"Length of each join key in the key pool (default: {DEFAULT_JOIN_KEY_SIZE})."
    )
    parser.add_argument(
        "-pks", "--primary_key_size",
        type=int,
        default=DEFAULT_PRIMARY_KEY_SIZE,
        help=f"Length of the primary key (pkey) in each table (default: {DEFAULT_PRIMARY_KEY_SIZE})."
    )
    parser.add_argument(
        "-vpr", "--versions_per_row",
        type=int,
        default=1,
        help=(
            f"Number of versions per row. If one value is provided, all tables get that row count. "
            f"Otherwise, specify one value per table. (default: {DEFAULT_NUM_ROWS})"
        )
    )
    parser.add_argument(
        "-deltas", "--delta_num",
        type=int,
        default=1,
        help=f"Number of deltas (default: 1)."
    )
    parser.add_argument(
        "-minv", "--min_value_size",
        type=int,
        default=DEFAULT_MIN_VALUE_SIZE,
        help=f"Minimum length of the 'value' column (default: {DEFAULT_MIN_VALUE_SIZE})."
    )
    parser.add_argument(
        "-maxv", "--max_value_size",
        type=int,
        default=DEFAULT_MAX_VALUE_SIZE,
        help=f"Maximum length of the 'value' column (default: {DEFAULT_MAX_VALUE_SIZE})."
    )
    parser.add_argument(
        "-scanjkn", "--scan_join_key_num",
        type=int,
        default=10,
        help=f"Maximum length of the 'value' column (default: {10})."
    )

    args = parser.parse_args()



    # -------------------------------------------------------------------------
    # (A) Generate the Join Key Pool
    # -------------------------------------------------------------------------
    print(f"Generating a join key pool of size {args.join_key_pool_size} (each key length = {args.join_key_size}) ...")
    join_key_pool = generate_unique_keys(args.join_key_pool_size, args.join_key_size)

    key_pool_path = os.path.join(CSV_DIR, "key_pool.csv")
    with open(key_pool_path, "w", newline="") as kpfile:
        writer = csv.writer(kpfile)
        writer.writerow(["join_key"])  # Column header
        for k in join_key_pool:
            writer.writerow([k])

    print(f"  -> {key_pool_path} (contains {args.join_key_pool_size} keys).")

    # -------------------------------------------------------------------------
    # (B) Generate All Deltas
    # -------------------------------------------------------------------------
    
    max_ts = gen_deltas_of_table(
        num_versions = args.versions_per_row,
        row_counts = args.primary_key_num,
        join_key_pool = join_key_pool,
        primary_key_size = args.primary_key_size,
        min_value_size = args.min_value_size,
        max_value_size = args.max_value_size,
        num_deltas = args.delta_num,
    )
    # -------------------------------------------------------------------------
    # (C) Generate Scan Keys
    # -------------------------------------------------------------------------
    gen_scan_keys(args.scan_join_key_num, join_key_pool=join_key_pool, ts_list = [0, max_ts])


if __name__ == "__main__":
    main()
