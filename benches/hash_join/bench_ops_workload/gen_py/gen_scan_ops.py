import csv
import argparse
import random

def parse_arguments():
    parser = argparse.ArgumentParser(
        description='Generate scan operations and expected results.',
        formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument(
        '--recent_data_file',
        default='../csv/recent_data_after_txs.csv',
        help='Input recent data file. (default: recent_data_after_txs.csv)'
    )
    parser.add_argument(
        '--history_data_file',
        default='../csv/history_data_after_txs.csv',
        help='Input history data file. (default: history_data_after_txs.csv)'
    )
    parser.add_argument(
        '--scan_ops_file',
        default='../csv/scan_ops.csv',
        help='Output scan operations file. (default: scan_ops.csv)'
    )
    parser.add_argument(
        '--num_scans',
        type=int,
        default=5,
        help='Number of scan operations to generate. (default: 5)'
    )
    return parser.parse_args()

def read_data_files(recent_data_file, history_data_file):
    data_entries = []

    # Read recent data
    with open(recent_data_file, 'r') as csvfile:
        reader = csv.reader(csvfile)
        for row in reader:
            if len(row) < 5:
                continue  # Skip invalid rows
            start_ts, end_ts, key, pkey, value = row
            data_entries.append({
                'start_ts': int(start_ts),
                'end_ts': int(end_ts) if end_ts != '-1' else float('inf'),
                'key': key,
                'pkey': pkey,
                'value': value
            })

    # Read history data
    with open(history_data_file, 'r') as csvfile:
        reader = csv.reader(csvfile)
        for row in reader:
            if len(row) < 5:
                continue  # Skip invalid rows
            start_ts, end_ts, key, pkey, value = row
            data_entries.append({
                'start_ts': int(start_ts),
                'end_ts': int(end_ts) if end_ts != '-1' else float('inf'),
                'key': key,
                'pkey': pkey,
                'value': value
            })

    return data_entries

def get_all_timestamps(data_entries):
    timestamps = set()
    for entry in data_entries:
        timestamps.add(entry['start_ts'])
        if entry['end_ts'] != float('inf'):
            timestamps.add(entry['end_ts'])
    return sorted(timestamps)

def generate_random_timestamps(all_timestamps, num_scans):
    min_ts = min(all_timestamps)
    max_ts = max(all_timestamps)
    # Exclude inf from possible timestamps
    valid_timestamps = [ts for ts in all_timestamps if ts != float('inf')]

    # Include the most recent timestamp (-1)
    random_timestamps = random.sample(valid_timestamps, min(num_scans - 1, len(valid_timestamps)))
    random_timestamps.append(-1)  # Representing the most recent timestamp
    return sorted(random_timestamps)

def collect_entries_at_timestamp(data_entries, ts):
    entries = []
    for entry in data_entries:
        start_ts = entry['start_ts']
        end_ts = entry['end_ts']
        if start_ts <= ts < end_ts:
            entries.append({
                'key': entry['key'],
                'pkey': entry['pkey'],
                'value': entry['value']
            })
    return entries

def write_scan_operations(scan_operations, scan_ops_file):
    with open(scan_ops_file, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        for scan_op in scan_operations:
            ts = scan_op['timestamp']
            writer.writerow(['scan', 'ts', ts])
            for entry in scan_op['entries']:
                writer.writerow([entry['key'], entry['pkey'], entry['value']])
            writer.writerow([])  # Blank line to separate scans

def main():
    args = parse_arguments()

    data_entries = read_data_files(args.recent_data_file, args.history_data_file)
    all_timestamps = get_all_timestamps(data_entries)
    scan_timestamps = generate_random_timestamps(all_timestamps, args.num_scans)

    scan_operations = []
    for ts in scan_timestamps:
        if ts == -1:
            effective_ts = float('inf')
        else:
            effective_ts = ts
        entries = collect_entries_at_timestamp(data_entries, effective_ts)
        scan_operations.append({
            'timestamp': ts,
            'entries': entries
        })

    write_scan_operations(scan_operations, args.scan_ops_file)
    print(f"Generated {len(scan_operations)} scan operations in {args.scan_ops_file}")

if __name__ == '__main__':
    main()
