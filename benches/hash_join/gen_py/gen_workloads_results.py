import os
import glob
import subprocess
import filecmp

# Paths
csv_dir = 'csv'
data_file = os.path.join(csv_dir, 'data.csv')
gen_data_script = './gen_py/gen_data_after_txs.py'

# Find all workloads by looking for *_ops.csv in the csv directory
ops_files = glob.glob(os.path.join(csv_dir, '*_ops.csv'))
ops_files.sort()

for ops_path in ops_files:
    # Derive workload_name from ops_file
    # e.g. insert_only_ops.csv -> workload_name: insert_only
    base_name = os.path.basename(ops_path)
    workload_name = base_name.replace('_ops.csv', '')

    txs_path = os.path.join(csv_dir, f'{workload_name}_txs.csv')
    if not os.path.exists(txs_path):
        print(f"No txs file found for workload {workload_name}, skipping...")
        continue

    # We'll produce workload-specific output files
    # For after txs
    recent_data_after_txs_file = os.path.join(csv_dir, f'{workload_name}_recent_data_after_txs.csv')
    history_data_after_txs_file = os.path.join(csv_dir, f'{workload_name}_history_data_after_txs.csv')

    # For after ops
    recent_data_after_ops_file = os.path.join(csv_dir, f'{workload_name}_recent_data_after_ops.csv')
    history_data_after_ops_file = os.path.join(csv_dir, f'{workload_name}_history_data_after_ops.csv')

    # Run gen_data_after_txs.py
    cmd = [
        'python3', gen_data_script,
        '-df', data_file,
        '-tf', txs_path,
        '-of', ops_path,
        '-rdatf', recent_data_after_txs_file,
        '-hdatf', history_data_after_txs_file,
        '-rdaof', recent_data_after_ops_file,
        '-hdaof', history_data_after_ops_file
    ]

    print(f"Processing workload {workload_name} with gen_data_after_txs.py...")
    subprocess.run(cmd, check=True)

    # Compare after txs and after ops files
    # If both recent_data_after_txs_file == recent_data_after_ops_file
    # and history_data_after_txs_file == history_data_after_ops_file
    # then delete *_txs.csv

    recent_same = False
    history_same = False

    if os.path.exists(recent_data_after_txs_file) and os.path.exists(recent_data_after_ops_file):
        recent_same = filecmp.cmp(recent_data_after_txs_file, recent_data_after_ops_file, shallow=False)

    if os.path.exists(history_data_after_txs_file) and os.path.exists(history_data_after_ops_file):
        history_same = filecmp.cmp(history_data_after_txs_file, history_data_after_ops_file, shallow=False)

    if recent_same and history_same:
        # If identical, delete *_txs.csv
        print(f"After txs and ops are identical for workload {workload_name}, deleting {txs_path}")
        os.remove(history_data_after_txs_file)
        os.remove(recent_data_after_txs_file)
    else:
        print(f"After txs and ops differ for workload {workload_name}, keeping {txs_path}")

print("Done processing all workloads.")
