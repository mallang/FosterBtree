#!/usr/bin/env python3

import csv
import argparse

def generate_recent_and_history_data(
    data_file,
    txs_file,
    ops_file,
    recent_data_after_txs_file,
    history_data_after_txs_file,
    recent_data_after_ops_file,
    history_data_after_ops_file
):
    # Initialize constants
    # MAX_TIMESTAMP = 18446744073709551615  # Representing u64::MAX
    MAX_TIMESTAMP = -1

    # Load initial data into recent_data_dict
    def load_initial_data():
        recent_data_dict = {}  # Maps pkey to (key, value, start_ts, end_ts)
        return recent_data_dict

    # Function to process operations and generate recent and history data
    def process_operations(ops_file, init_data_dict):
        # Initialize history data list
        history_data_list = []  # List of (start_ts, end_ts, key, pkey, value)

        # Read operations and apply them
        with open(ops_file, 'r') as csvfile:
            reader = csv.reader(csvfile)
            for row in reader:
                if not row or len(row) < 6:
                    continue  # Skip empty or invalid lines
                tx_id_str, ts_str, op_type, key, pkey, value = row
                tx_id = int(tx_id_str)
                ts = int(ts_str)
                op_type = op_type.lower()

                if op_type == 'insert':
                    if pkey in init_data_dict:
                        # Insertion of existing pkey; move existing record to history
                        # old_key, old_value, old_start_ts, old_end_ts = recent_data_dict[pkey]
                        # history_data_list.append((old_start_ts, ts, old_key, pkey, old_value))
                        # actually it shouldnt happen, panic
                        print(f"Warning: Insert operation on existing pkey {pkey}. Ignoring.")
                    # Insert new record into recent data
                    init_data_dict[pkey] = (key, value, ts, MAX_TIMESTAMP)
                elif op_type == 'update':
                    if pkey in init_data_dict:
                        # Move existing record to history
                        old_key, old_value, old_start_ts, old_end_ts = init_data_dict[pkey]
                        history_data_list.append((old_start_ts, ts, old_key, pkey, old_value))
                        # Update recent data with new value and timestamp
                        init_data_dict[pkey] = (key, value, ts, MAX_TIMESTAMP)
                    else:
                        print(f"Warning: Update operation on non-existent pkey {pkey}. Ignoring.")
                elif op_type == 'delete':
                    if pkey in init_data_dict:
                        # Move existing record to history
                        old_key, old_value, old_start_ts, old_end_ts = init_data_dict[pkey]
                        history_data_list.append((old_start_ts, ts, old_key, pkey, old_value))
                        # Remove from recent data
                        del init_data_dict[pkey]
                    else:
                        print(f"Warning: Delete operation on non-existent pkey {pkey}. Ignoring.")
                elif op_type == 'get' or op_type == 'commit' or op_type == 'scan':
                    # No changes to data
                    continue
                else:
                    print(f"Warning: Unknown operation type '{op_type}' in operation {row}. Ignoring.")
        return init_data_dict, history_data_list

    # Function to extract numeric value from pkey for sorting
    def pkey_numeric_value(pkey):
        # Remove underscores and leading zeros, then convert to integer
        numeric_part = pkey.replace('_', '').lstrip('0')
        if numeric_part == '':
            numeric_part = '0'  # Handle cases where pkey is all underscores or zeros
        return int(numeric_part)

    # Function to write recent and history data to files
    def write_data_files(recent_data_dict, history_data_list, recent_data_file, history_data_file):
        # Write recent data to recent_data_file
        with open(recent_data_file, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            # Sort recent data by numeric value of pkey
            sorted_pkeys = sorted(recent_data_dict.keys(), key=pkey_numeric_value)
            for pkey in sorted_pkeys:
                key, value, start_ts, end_ts = recent_data_dict[pkey]
                # Place ts in the first columns
                writer.writerow([start_ts, end_ts, key, pkey, value])

        print(f"Generated recent data in {recent_data_file}")

        # Write history data to history_data_file
        with open(history_data_file, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            # Sort history data by numeric value of pkey and start_ts
            history_data_list.sort(key=lambda x: (pkey_numeric_value(x[3]), x[0]))  # x[3] is pkey, x[0] is start_ts
            for record in history_data_list:
                start_ts, end_ts, key, pkey, value = record
                # Place ts in the first columns
                writer.writerow([start_ts, end_ts, key, pkey, value])

        print(f"Generated history data in {history_data_file}")

    # Process txs.csv
    print("\nProcessing transactions (txs.csv)...")
    init_data_dict = load_initial_data()
    recent_data_after_txs_dict, history_data_after_txs_list = process_operations(data_file, init_data_dict.copy())
    write_data_files(
        recent_data_after_txs_dict,
        history_data_after_txs_list,
        recent_data_after_txs_file,
        history_data_after_txs_file
    )

    # Process ops.csv
    print("\nProcessing operations (ops.csv)...")
    init_data_dict = load_initial_data()
    recent_data_after_ops_dict, history_data_after_ops_list = process_operations(data_file, init_data_dict.copy())
    write_data_files(
        recent_data_after_ops_dict,
        history_data_after_ops_list,
        recent_data_after_ops_file,
        history_data_after_ops_file
    )

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Generate recent and history data after applying transactions and operations.',
        formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument(
        '-df', '--data_file',
        default='../csv/data.csv',
        help='Input data file containing initial keys, pkeys, and values. (default: data.csv)'
    )
    parser.add_argument(
        '-tf', '--txs_file',
        default='../csv/txs.csv',
        help='Input transactions file (txs.csv). (default: txs.csv)'
    )
    parser.add_argument(
        '-of', '--ops_file',
        default='../csv/ops.csv',
        help='Input operations file (ops.csv). (default: ops.csv)'
    )
    parser.add_argument(
        '-rdatf', '--recent_data_after_txs_file',
        default='../csv/recent_data_after_txs.csv',
        help='Output file to write the recent data after applying transactions. (default: recent_data_after_txs.csv)'
    )
    parser.add_argument(
        '-hdatf', '--history_data_after_txs_file',
        default='../csv/history_data_after_txs.csv',
        help='Output file to write the history data after applying transactions. (default: history_data_after_txs.csv)'
    )
    parser.add_argument(
        '-rdaof', '--recent_data_after_ops_file',
        default='../csv/recent_data_after_ops.csv',
        help='Output file to write the recent data after applying operations. (default: recent_data_after_ops.csv)'
    )
    parser.add_argument(
        '-hdaof', '--history_data_after_ops_file',
        default='../csv/history_data_after_ops.csv',
        help='Output file to write the history data after applying operations. (default: history_data_after_ops.csv)'
    )
    args = parser.parse_args()

    generate_recent_and_history_data(
        data_file=args.data_file,
        txs_file=args.txs_file,
        ops_file=args.ops_file,
        recent_data_after_txs_file=args.recent_data_after_txs_file,
        history_data_after_txs_file=args.history_data_after_txs_file,
        recent_data_after_ops_file=args.recent_data_after_ops_file,
        history_data_after_ops_file=args.history_data_after_ops_file
    )
