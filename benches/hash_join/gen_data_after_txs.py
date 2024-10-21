#!/usr/bin/env python3

import csv
import argparse

def generate_final_data(data_file, txs_file, final_data_file):
    # Load initial data into a dictionary
    data_dict = {}  # Maps pkey to (key, value)
    with open(data_file, 'r') as csvfile:
        reader = csv.reader(csvfile)
        for row in reader:
            if len(row) < 3:
                continue  # Skip invalid rows
            key, pkey, value = row
            data_dict[pkey] = (key, value)

    # Read transactions and apply operations
    with open(txs_file, 'r') as csvfile:
        reader = csv.reader(csvfile)
        current_tx_id = None
        tx_commands = []
        for row in reader:
            if not row:
                continue  # Skip empty lines
            tx_id, ts, op_type, key, pkey, value = row
            tx_id = int(tx_id)
            ts = int(ts)
            cmd = {
                'tx_id': tx_id,
                'ts': ts,
                'op_type': op_type,
                'key': key,
                'pkey': pkey,
                'value': value
            }
            if current_tx_id is None:
                current_tx_id = tx_id

            if tx_id != current_tx_id:
                # Apply the previous transaction
                tx_commands.sort(key=lambda x: x['ts'])
                apply_transaction(tx_commands, data_dict)
                tx_commands = [cmd]
                current_tx_id = tx_id
            else:
                tx_commands.append(cmd)

        # Apply the last transaction
        if tx_commands:
            tx_commands.sort(key=lambda x: x['ts'])
            apply_transaction(tx_commands, data_dict)

    # Write final data to final_data.csv
    with open(final_data_file, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        for pkey in sorted(data_dict.keys()):
            key, value = data_dict[pkey]
            writer.writerow([key, pkey, value])

    print(f"Generated final data in {final_data_file}")

def apply_transaction(tx_commands, data_dict):
    for cmd in tx_commands:
        op_type = cmd['op_type']
        key = cmd['key']
        pkey = cmd['pkey']
        value = cmd['value']
        if op_type == 'insert':
            if pkey in data_dict:
                print(f"Warning: Insert operation on existing pkey {pkey}. Overwriting.")
            data_dict[pkey] = (key, value)
        elif op_type == 'update':
            if pkey in data_dict:
                data_dict[pkey] = (key, value)
            else:
                print(f"Warning: Update operation on non-existent pkey {pkey}. Ignoring.")
        elif op_type == 'delete':
            if pkey in data_dict:
                del data_dict[pkey]
            else:
                print(f"Warning: Delete operation on non-existent pkey {pkey}. Ignoring.")
        elif op_type == 'get':
            pass  # Reads don't modify the data

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Generate final data after applying transactions.',
        formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument(
        '-df', '--data_file',
        default='data.csv',
        help='Input data file containing initial keys, pkeys, and values. (default: data.csv)'
    )
    parser.add_argument(
        '-tf', '--txs_file',
        default='txs.csv',
        help='Input transactions file containing operations. (default: txs.csv)'
    )
    parser.add_argument(
        '-of', '--final_data_file',
        default='data_after_txs.csv',
        help='Output file to write the final data after applying transactions. (default: data_after_txs.csv)'
    )
    args = parser.parse_args()

    generate_final_data(
        data_file=args.data_file,
        txs_file=args.txs_file,
        final_data_file=args.final_data_file
    )
