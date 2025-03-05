#!/usr/bin/env python3
import os
import csv
import sys
import re
import glob
import copy

# ------------------------------------------------------------------------------
# Setup directories: CSV_DIR is set relative to this script and RES_DIR is a subfolder.
# ------------------------------------------------------------------------------
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
CSV_DIR = os.path.join(SCRIPT_DIR, "..", "csv")
RES_DIR = os.path.join(CSV_DIR, "res")
if not os.path.exists(RES_DIR):
    os.makedirs(RES_DIR, exist_ok=True)
    print(f"[INFO] Created results directory: {RES_DIR}")

# Remove any existing CSV files in RES_DIR
for f in glob.glob(os.path.join(RES_DIR, "*.csv")):
    os.remove(f)
print(f"[INFO] Removed existing CSV files in {RES_DIR}")

# ------------------------------------------------------------------------------
# Functions for normal TX log processing (non-join)
# ------------------------------------------------------------------------------

def read_table(table_file):
    """
    Reads the base table CSV (expected header: pkey, join_key, value).
    Returns a dictionary mapping each pkey to a list of version records.
    Each record is a dict with keys: start_ts, end_ts, pkey, join_key, value.
    Initially, start_ts is 0 and end_ts is empty.
    """
    table_versions = {}
    with open(table_file, "r", newline="") as f:
        reader = csv.DictReader(f)
        required = {"pkey", "join_key", "value"}
        if not required.issubset(reader.fieldnames):
            print(f"[ERROR] {table_file} must have columns: {', '.join(required)}")
            sys.exit(1)
        for row in reader:
            pkey = row["pkey"].strip()
            version = {
                "start_ts": 0,
                "end_ts": "",
                "pkey": pkey,
                "join_key": row["join_key"].strip(),
                "value": row["value"].strip()
            }
            table_versions[pkey] = [version]
    return table_versions

def apply_txs(tx_file, table_versions):
    """
    Reads a TX log CSV (expected header: tx_id, ts, op, pkey, join_key, value)
    and applies operations (insert, update, delete) to update table_versions.
    Returns the updated table_versions.
    """
    with open(tx_file, "r", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            op = row["op"].strip().lower()
            try:
                ts = int(row["ts"].strip())
            except ValueError:
                continue
            pkey = row["pkey"].strip()
            join_key = row["join_key"].strip()
            value = row["value"].strip()
            if op == "insert":
                version = {"start_ts": ts, "end_ts": "", "pkey": pkey, "join_key": join_key, "value": value}
                table_versions[pkey] = [version]
            elif op == "update":
                if pkey in table_versions:
                    current = table_versions[pkey][-1]
                    if current["end_ts"] == "":
                        current["end_ts"] = ts
                    new_version = {"start_ts": ts, "end_ts": "", "pkey": pkey, "join_key": join_key, "value": value}
                    table_versions[pkey].append(new_version)
                else:
                    version = {"start_ts": ts, "end_ts": "", "pkey": pkey, "join_key": join_key, "value": value}
                    table_versions[pkey] = [version]
            elif op == "delete":
                if pkey in table_versions:
                    current = table_versions[pkey][-1]
                    if current["end_ts"] == "":
                        current["end_ts"] = ts
            # Ignore other operations.
    return table_versions

def write_results(table_versions, recent_out, history_out):
    """
    Writes two CSV files:
      - recent_out: All version records with an empty end_ts.
      - history_out: All records with a non-empty end_ts.
    Both files have header: start_ts, end_ts, pkey, join_key, value.
    """
    recent_rows = []
    history_rows = []
    for versions in table_versions.values():
        for ver in versions:
            if ver["end_ts"] == "":
                recent_rows.append(ver)
            else:
                history_rows.append(ver)
    with open(recent_out, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["start_ts", "end_ts", "pkey", "join_key", "value"])
        writer.writeheader()
        for row in recent_rows:
            writer.writerow(row)
    with open(history_out, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["start_ts", "end_ts", "pkey", "join_key", "value"])
        writer.writeheader()
        for row in history_rows:
            writer.writerow(row)

# ------------------------------------------------------------------------------
# Functions for join TX log processing (scanning update results)
# ------------------------------------------------------------------------------

def scan_update_results(final_state, join_key, ts):
    """
    Given final_state (a list of version records from update results),
    returns those records valid at time ts (i.e. int(start_ts) <= ts and
    (end_ts is empty or ts < int(end_ts))) and whose join_key matches.
    """
    result = []
    for rec in final_state:
        try:
            st = int(rec["start_ts"])
        except ValueError:
            continue
        et = int(rec["end_ts"]) if rec["end_ts"].strip() != "" else None
        if st <= ts and (et is None or ts < et) and rec["join_key"] == join_key:
            result.append(rec)
    return result

def process_join_log_for_update_label(txs_file, table_idx, update_label):
    """
    Processes a join TX log file (expected header: tx_id, ts, op, join_key) using the update result for a given update_label.
    For each TX row in the join log, perform a scan (without grouping) and append its results (with an extra blank row for readability)
    into one output file named: res_join_{update_label}_ts{ts_from_log}_t{table_idx}.csv.
    Here, ts_from_log is extracted from each TX row.
    """
    # Read update results for this update label (from both recent and history files)
    recent_file = os.path.join(RES_DIR, f"res_{update_label}_t0_r.csv")
    history_file = os.path.join(RES_DIR, f"res_{update_label}_t0_h.csv")
    final_state = []
    for fname in [recent_file, history_file]:
        if not os.path.exists(fname):
            print(f"[WARN] Update result file {fname} not found. Skipping join log {txs_file} for update label {update_label}.")
            return
        with open(fname, "r", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                final_state.append(row)
    # Open the join TX log file (expected header: tx_id, ts, op, join_key)
    txs_path = os.path.join(CSV_DIR, txs_file)
    if not os.path.exists(txs_path):
        print(f"[WARN] Join TX log file {txs_file} not found in CSV_DIR.")
        return
    # For each TX row (ignoring tx_begin/tx_commit), scan update results and write output.
    # We will collect results in a dictionary keyed by ts.
    output_by_ts = {}  # key: ts (int) -> list of rows (each a dict)
    with open(txs_path, "r", newline="") as f:
        reader = csv.DictReader(f)
        if reader.fieldnames is None or len(reader.fieldnames) != 4:
            print(f"[WARN] {txs_file} is not in expected join log format (4 columns).")
            return
        for row in reader:
            op = row["op"].strip().lower()
            if op in ("tx_begin", "tx_commit"):
                continue
            try:
                ts_val = int(row["ts"].strip())
            except ValueError:
                continue
            join_key = row["join_key"].strip()
            # For each TX row, perform a scan
            scanned = scan_update_results(final_state, join_key, ts_val)
            # Format the scanned records; if none found, leave list empty.
            if ts_val not in output_by_ts:
                output_by_ts[ts_val] = []
            # Append a block: first a header-like comment, then the scanned rows, then a blank row.
            header_block = {"start_ts": f"--- Scan for join_key: {join_key} at ts={ts_val} ---",
                            "end_ts": "", "pkey": "", "join_key": "", "value": ""}
            output_by_ts[ts_val].append(header_block)
            output_by_ts[ts_val].extend(scanned)
            # Append a blank row for readability.
            # output_by_ts[ts_val].append({"start_ts": "", "end_ts": "", "pkey": "", "join_key": "", "value": ""})
    
    # Now, for each distinct ts, write one output file.
    for ts_val, rows in output_by_ts.items():
        out_filename = f"res_join_{update_label}_ts{ts_val}_t{table_idx}.csv"
        out_path = os.path.join(RES_DIR, out_filename)
        with open(out_path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=["start_ts", "end_ts", "pkey", "join_key", "value"])
            writer.writeheader()
            for rec in rows:
                writer.writerow(rec)
        print(f"Join log processed: {txs_file} (ts={ts_val}) -> {out_path}")

# ------------------------------------------------------------------------------
# Main driver: Process all TX log files in CSV_DIR.
# ------------------------------------------------------------------------------
def main():
    print(f"Using CSV directory: {CSV_DIR}")
    if not os.path.exists(CSV_DIR):
        print(f"[ERROR] CSV directory not found: {CSV_DIR}")
        sys.exit(1)
    
    # Discover all TX log files matching pattern: txs_(.+)_t(\d+).csv
    txs_pattern = re.compile(r"^txs_(.+)_t(\d+)\.csv$")
    all_files = os.listdir(CSV_DIR)
    txs_files = []
    for f in all_files:
        m = txs_pattern.match(f)
        if m:
            coverage_label = m.group(1)  # e.g., "u0.5" or "join_ts10"
            table_idx = m.group(2)       # e.g., "0", "1", etc.
            txs_files.append((f, coverage_label, table_idx))
    
    if not txs_files:
        print("No TX log files found matching pattern txs_*_t{i}.csv")
        sys.exit(0)
    
    print("Found the following TX log files:")
    for (f, cl, tid) in txs_files:
        print(f"  File: {f}, coverage label: {cl}, table index: {tid}")
    
    # Partition TX logs into normal and join logs.
    normal_logs = []
    join_logs = []
    for (f, cl, tid) in txs_files:
        if cl.startswith("join_"):
            join_logs.append((f, cl, tid))
        else:
            normal_logs.append((f, cl, tid))
    
    # Process normal TX logs.
    for (f, cl, tid) in normal_logs:
        base_table_file = os.path.join(CSV_DIR, f"table_{tid}.csv")
        if not os.path.exists(base_table_file):
            print(f"[WARN] Base table file table_{tid}.csv not found. Skipping {f}.")
            continue
        print(f"\nProcessing TX log: {f} for base table: table_{tid}.csv")
        base_tables_cache = {}
        txs_path = os.path.join(CSV_DIR, f)
        if tid not in base_tables_cache:
            base_tables_cache[tid] = read_table( os.path.join(CSV_DIR, f"table_{tid}.csv") )
        table_versions = copy.deepcopy(base_tables_cache[tid])
        apply_txs( txs_path, table_versions )
        table_versions = read_table(base_table_file)
        table_versions = apply_txs(txs_path, table_versions)
        recent_out = os.path.join(RES_DIR, f"res_{cl}_t{tid}_r.csv")
        history_out = os.path.join(RES_DIR, f"res_{cl}_t{tid}_h.csv")
        write_results(table_versions, recent_out, history_out)
        print(f"Processed {f}:")
        print(f"   Recent versions -> {recent_out}")
        print(f"   History versions -> {history_out}")
    
    # For join TX logs, instead of choosing only one update label, we process for all update labels.
    # Find all update labels from RES_DIR (files matching res_(u[\d.]+)_t0_r.csv)
    update_pattern = re.compile(r"^res_(u[\d.]+)_t0_r\.csv$")
    update_labels = []
    for f in os.listdir(RES_DIR):
        m = update_pattern.match(f)
        if m:
            label = m.group(1)
            update_labels.append(label)
    if not update_labels:
        print("[WARN] No update result files found in RES_DIR. Skipping join TX logs.")
    else:
        update_labels = sorted(update_labels, key=lambda x: float(x[1:]))  # sort by numeric value (ignoring leading 'u')
        print(f"[INFO] Found update labels: {update_labels}")
        # Process each join TX log for each update label.
        for (f, cl, tid) in join_logs:
            for ul in update_labels:
                print(f"\nProcessing JOIN TX log: {f} (coverage label={cl}) using update label '{ul}' for table index {tid}")
                process_join_log_for_update_label(f, tid, ul)
    
    print("\nAll done!")

if __name__ == "__main__":
    main()
