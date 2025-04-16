#!/usr/bin/env python3

import os
import sys
import argparse
import csv
import math
import subprocess

# ------------------------------------------------------------------------------
# Default Config
# ------------------------------------------------------------------------------
DEFAULT_TABLE0 = "table_0.csv"

DEFAULT_MIN_COVERAGE = 0.1   # 10%
DEFAULT_MAX_COVERAGE = 1.0   # 100%
DEFAULT_STEP_COVERAGE = 0.1  # step of 0.1 => 10% increments

DEFAULT_NUM_TRANSACTIONS = 2  # coverage logs: number of TX per file
DEFAULT_INSERT_RATIO = 0
DEFAULT_GET_RATIO    = 0
DEFAULT_UPDATE_RATIO = 1
DEFAULT_DELETE_RATIO = 0

# For discovered join tables
DEFAULT_TX_JOIN_TABLES = [1]
DEFAULT_TS_VALUES = [1, 100]  # Default ts value(s) for join logs

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
CSV_DIR = os.path.join(SCRIPT_DIR, "..", "csv")


def pick_min_max(approx_ops):
    """
    Convert a float 'approx_ops' into (min_ops, max_ops) with a small rounding strategy.
    - If approx_ops < 1 => clamp to (1,1).
    - If fraction part <= 0.25 => (base_int, base_int)
    - If fraction part <= 0.75 => (base_int, base_int+1)
    - Else => (base_int+1, base_int+1)
    """
    if approx_ops < 1.0:
        return 1, 1
    base_int = int(math.floor(approx_ops))
    frac = approx_ops - base_int
    if frac <= 0.25:
        return max(1, base_int), max(1, base_int)
    elif frac <= 0.75:
        return max(1, base_int), max(1, base_int + 1)
    else:
        return max(1, base_int + 1), max(1, base_int + 1)


def main():
    parser = argparse.ArgumentParser(description="Generate incremental coverage logs for table_0, then join logs.")
    
    # Table_0 filename
    parser.add_argument("-t0", "--table0", default=DEFAULT_TABLE0,
                        help=f"Base table_0 CSV filename (default: {DEFAULT_TABLE0}).")
    
    # Coverage fraction configuration
    parser.add_argument("-mintx", "--min_coverage_fraction", type=float,
                        default=DEFAULT_MIN_COVERAGE,
                        help=f"Minimum coverage fraction (default: {DEFAULT_MIN_COVERAGE}).")
    parser.add_argument("-maxtx", "--max_coverage_fraction", type=float,
                        default=DEFAULT_MAX_COVERAGE,
                        help=f"Maximum coverage fraction (default: {DEFAULT_MAX_COVERAGE}).")
    parser.add_argument("-steptx", "--step_coverage_fraction", type=float,
                        default=DEFAULT_STEP_COVERAGE,
                        help=f"Increment step for coverage fraction (default: {DEFAULT_STEP_COVERAGE}).")
    
    parser.add_argument("-ntx", "--num_transactions", type=int,
                        default=DEFAULT_NUM_TRANSACTIONS,
                        help=f"Number of TX for each coverage log (default: {DEFAULT_NUM_TRANSACTIONS}).")
    
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
    
    # For discovered join tables: number of TX per join file
    parser.add_argument("-tjt", "--txs_join_tables", nargs="+", type=int,
                        default=DEFAULT_TX_JOIN_TABLES,
                        help=("Number of transactions for discovered join tables. "
                              f"(default: {DEFAULT_TX_JOIN_TABLES})"))
    # New: ts values list for join logs.
    parser.add_argument("--ts-values", nargs="+", type=int,
                        default=DEFAULT_TS_VALUES,
                        help=("List of starting ts values for join logs. For each discovered join table, "
                              "a file is generated for each ts value. (default: %(default)s)"))
    
    args = parser.parse_args()
    
    # Ensure CSV dir
    if not os.path.exists(CSV_DIR):
        os.makedirs(CSV_DIR, exist_ok=True)
        print(f"[INFO] Created CSV dir: {CSV_DIR}")

    table0_path = os.path.join(CSV_DIR, args.table0)
    if not os.path.exists(table0_path):
        print(f"[ERROR] {table0_path} not found.")
        sys.exit(1)
    
    # 1) Read row_count of table_0
    row_count_table_0 = 0
    with open(table0_path, "r", newline="") as f:
        reader = csv.DictReader(f)
        if "pkey" not in reader.fieldnames:
            print("[ERROR] table_0 must have 'pkey' column.")
            sys.exit(1)
        for _ in reader:
            row_count_table_0 += 1
    
    if row_count_table_0 == 0:
        print(f"[ERROR] table_0 is empty => no coverage possible.")
        sys.exit(1)
    
    print(f"table_0 has {row_count_table_0} rows.")
    
    # 2) Build coverage fraction range from config
    coverage_fractions = []
    frac = args.min_coverage_fraction
    while frac <= args.max_coverage_fraction + 1e-9:
        coverage_fractions.append(frac)
        frac += args.step_coverage_fraction
    
    coverage_output_files = []
    for coverage_frac in coverage_fractions:
        coverage_count = int(round(coverage_frac * row_count_table_0))
        coverage_pct = coverage_frac * 100
        num_tx = args.num_transactions if args.num_transactions > 0 else 1
        
        if coverage_count <= 0:
            print(f"\n[WARN] coverage_count=0 for fraction={coverage_frac:.3f}. Skipping file.")
            continue
        
        approx_ops = coverage_count / float(num_tx)
        minop, maxop = pick_min_max(approx_ops)
        
        # New filename format: txs_u{coverage_frac:.1f}_t0.csv
        out_filename = f"txs_u{coverage_frac:.1f}_t0.csv"
        out_path = os.path.join(CSV_DIR, out_filename)
        print(f"\nCoverage fraction={coverage_frac:.3f} (~{coverage_pct:.1f}%), coverage_count={coverage_count}, "
              f"avg={approx_ops:.2f}, minop={minop}, maxop={maxop}.")
        print(f" => Generating {out_filename} with {num_tx} TX(s).")
        
        # Call gen_txs_t0.py with the proper arguments
        cmd = [
            "python3", os.path.join(SCRIPT_DIR, "gen_txs_t0.py"),
            "--table0", args.table0,
            "--txs0-out", out_filename,
            "-ntx", str(num_tx),
            "-minop", str(minop),
            "-maxop", str(maxop),
            "-ir", str(args.insert_ratio),
            "-gr", str(args.get_ratio),
            "-ur", str(args.update_ratio),
            "-dr", str(args.delete_ratio)
        ]
        print("Command:", " ".join(cmd))
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0:
            print(f"[ERROR] Failed generating coverage fraction={coverage_frac:.3f}.")
            print("STDERR:", result.stderr)
        else:
            print(result.stdout)
            coverage_output_files.append(out_filename)
    
    # 3) Call gen_txs_join.py for discovered join tables.
    #    We now pass both -tjt and --ts-values.
    print("\nNow generating join logs with gen_txs_join.py ...")
    cmd_join = [
        "python3",
        os.path.join(SCRIPT_DIR, "gen_txs_join.py"),
        "--num_tx"
    ]
    # Add the join TX counts
    cmd_join.extend([str(x) for x in args.txs_join_tables])
    # cmd_join.extend(["1"])
    # Add the ts-values argument
    cmd_join.extend(["--ts-values"])
    cmd_join.extend([str(x) for x in args.ts_values])
    
    print("Command:", " ".join(cmd_join))
    res_join = subprocess.run(cmd_join, capture_output=True, text=True)
    if res_join.returncode != 0:
        print("[ERROR] gen_txs_join.py failed.")
        print("STDERR:", res_join.stderr)
    else:
        print(res_join.stdout)
    
    print("\nAll done!")
    print("Coverage logs generated:", ", ".join(coverage_output_files))
    print("Join logs (if discovered) also generated.")


if __name__ == "__main__":
    main()
