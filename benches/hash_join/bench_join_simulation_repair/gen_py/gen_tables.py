import os
import argparse
import csv
import random
import string
import sys

# =============================================================================
# 1) Default Constants
# =============================================================================

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
# 3) Main Logic
# =============================================================================

def main():
    parser = argparse.ArgumentParser(description="Generate key pool and multiple tables for hash join simulation.")
    
    parser.add_argument(
        "-T", "--num_tables",
        type=int,
        default=DEFAULT_NUM_TABLES,
        help=f"Number of tables to generate (default: {DEFAULT_NUM_TABLES})."
    )
    parser.add_argument(
        "-r", "--rows",
        type=int,
        nargs="+",   # allow multiple values
        default=[DEFAULT_NUM_ROWS],
        help=(
            f"Number of rows per table. If one value is provided, all tables get that row count. "
            f"Otherwise, specify one value per table. (default: {DEFAULT_NUM_ROWS})"
        )
    )
    parser.add_argument(
        "-jks", "--join_key_pool_size",
        type=int,
        default=DEFAULT_JOIN_KEY_POOL_SIZE,
        help=f"Number of distinct join keys to include in the key pool (default: {DEFAULT_JOIN_KEY_POOL_SIZE})."
    )
    parser.add_argument(
        "-jk", "--join_key_size",
        type=int,
        default=DEFAULT_JOIN_KEY_SIZE,
        help=f"Length of each join key in the key pool (default: {DEFAULT_JOIN_KEY_SIZE})."
    )
    parser.add_argument(
        "-pk", "--primary_key_size",
        type=int,
        default=DEFAULT_PRIMARY_KEY_SIZE,
        help=f"Length of the primary key (pkey) in each table (default: {DEFAULT_PRIMARY_KEY_SIZE})."
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
    
    args = parser.parse_args()

    # Validate or expand row counts
    if len(args.rows) == 1:
        row_counts = [args.rows[0]] * args.num_tables
    elif len(args.rows) == args.num_tables:
        row_counts = args.rows
    else:
        print("ERROR: The number of row counts must be either 1 or equal to the number of tables.")
        sys.exit(1)

    # Ensure the CSV_DIR exists (or create it if you prefer)
    if not os.path.exists(CSV_DIR):
        os.makedirs(CSV_DIR, exist_ok=True)
        print(f"[INFO] Created CSV directory: {CSV_DIR}")

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
    # (B) Generate Each Table
    # -------------------------------------------------------------------------
    for t_index in range(args.num_tables):
        table_filename = os.path.join(CSV_DIR, f"table_{t_index}.csv")
        num_rows = row_counts[t_index]

        print(f"Generating {table_filename} with {num_rows} rows...")

        with open(table_filename, "w", newline="") as tf:
            writer = csv.writer(tf)
            writer.writerow(["pkey", "join_key", "value"])  # Column headers

            for _ in range(num_rows):
                # pkey: random string of length 'primary_key_size'
                pkey = ''.join(random.choices(string.ascii_letters + string.digits, k=args.primary_key_size))

                # join_key: randomly selected from the pool
                join_key = random.choice(join_key_pool)

                # value: Generate base string and attach length in parentheses
                base_value = generate_base_value()
                val_length = random.randint(args.min_value_size, args.max_value_size)
                value_data = f"{base_value}({val_length})"

                writer.writerow([pkey, join_key, value_data])

        print(f"  -> {table_filename} generated.")

    print("\nAll tables generated successfully!")
    print(f"Join key pool file: {key_pool_path}")
    for t_index in range(args.num_tables):
        print(f"  -> table_{t_index}.csv in {CSV_DIR}")


if __name__ == "__main__":
    main()
