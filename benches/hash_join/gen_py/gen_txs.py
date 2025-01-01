import csv
import argparse
import random
from collections import defaultdict
import string

def generate_transactions_and_operations(
    data_file,
    txs_file,
    ops_file,
    num_transactions,
    min_cmds_per_tx,
    max_cmds_per_tx,
    read_only_ratio,
    insert_ratio,
    update_ratio,
    delete_ratio,
    get_ratio,
    scan_ratio
):
    # Load data and determine pkey length
    data = []
    pkey_lengths = set()
    value_lengths = []  # To collect lengths of values
    with open(data_file, 'r') as csvfile:
        reader = csv.reader(csvfile)
        for row in reader:
            if len(row) < 3:
                continue  # Skip invalid rows
            key, pkey_str, value = row
            data.append((key, pkey_str, value))
            pkey_lengths.add(len(pkey_str))
            value_lengths.append(len(value))  # Collect value lengths

    # Ensure pkey lengths are consistent
    if len(pkey_lengths) != 1:
        raise ValueError("Inconsistent pkey lengths in data file.")
    else:
        pkey_length = pkey_lengths.pop()

    # Determine min and max value lengths from data
    if value_lengths:
        min_value_length = min(value_lengths)
        max_value_length = max(value_lengths)
    else:
        # If data is empty, use default lengths
        min_value_length = 32
        max_value_length = 64

    # Prepare for generating new pkeys
    existing_pkeys = set(pkey_str for _, pkey_str, _ in data)
    existing_pkey_numbers = [int(pkey.strip('_')) for pkey in existing_pkeys if pkey.strip('_').isdigit()]
    max_existing_pkey = max(existing_pkey_numbers) if existing_pkey_numbers else 0
    new_pkey_counter = max_existing_pkey + 1

    # Initialize the pool of pkeys for operations
    available_pkeys = set(existing_pkeys)
    pkey_info = {}  # Maps pkey to (key, pkey, value)

    for key, pkey, value in data:
        pkey_info[pkey] = (key, pkey, value)

    # Validate ratios
    total_ratio = insert_ratio + update_ratio + delete_ratio + get_ratio + scan_ratio
    if abs(total_ratio - 1.0) > 0.001:
        raise ValueError("Insert, update, delete, and get ratios must sum to 1.0")
    if not 0 <= read_only_ratio <= 1:
        raise ValueError("Read-only ratio must be between 0 and 1")

    total_read_only_txs = int(num_transactions * read_only_ratio)
    total_read_write_txs = num_transactions - total_read_only_txs

    # Generate transactions
    transactions = []
    tx_id = 0  # Start from 0 for consistency with op IDs
    ts = 0

    for _ in range(num_transactions):
        tx_id += 1
        ts += 1
        num_cmds = random.randint(min_cmds_per_tx, max_cmds_per_tx)
        commands = []
        is_read_only = random.random() < read_only_ratio
        for _ in range(num_cmds):
            if is_read_only:
                # Read-only transaction
                if not available_pkeys:
                    continue  # No available pkeys to read
                pkey = random.choice(list(available_pkeys))
                key, pkey_str, _ = pkey_info[pkey]
                commands.append({
                    'tx_id': tx_id,
                    'ts': ts,
                    'op_type': 'get',
                    'key': key,
                    'pkey': pkey_str,
                    'value': ''
                })
            else:
                # Read-write transaction
                op_type = random.choices(
                    ['insert', 'update', 'delete', 'get', 'scan'],
                    weights=[args.insert_ratio, args.update_ratio, args.delete_ratio, args.get_ratio, args.scan_ratio],
                    k=1
                )[0]
                if op_type == 'insert':
                    # Generate a new pkey with the same length, padded with underscores
                    new_pkey_str = str(new_pkey_counter)
                    if len(new_pkey_str) < pkey_length:
                        new_pkey = new_pkey_str.rjust(pkey_length, '_')
                    else:
                        new_pkey = new_pkey_str[:pkey_length]
                    new_pkey_counter += 1
                    # Select a key from the key pool
                    if data:
                        key = random.choice(data)[0]
                    else:
                        key = 'key'.ljust(pkey_length, '_')[:pkey_length]  # Default key if data is empty
                    # Generate a new value using min and max lengths
                    value_length = random.randint(min_value_length, max_value_length)
                    new_value = ''.join(random.choices(string.ascii_letters + string.digits, k=value_length))
                    commands.append({
                        'tx_id': tx_id,
                        'ts': ts,
                        'op_type': 'insert',
                        'key': key,
                        'pkey': new_pkey,
                        'value': new_value
                    })
                    # Add new pkey to available pkeys
                    available_pkeys.add(new_pkey)
                    pkey_info[new_pkey] = (key, new_pkey, new_value)
                else:
                    if not available_pkeys:
                        continue  # No pkeys available for update/delete/get
                    pkey = random.choice(list(available_pkeys))
                    key, pkey_str, value = pkey_info[pkey]
                    if op_type == 'update':
                        # Generate a new random value
                        value_length = random.randint(min_value_length, max_value_length)
                        updated_value = ''.join(random.choices(string.ascii_letters + string.digits, k=value_length))
                        commands.append({
                            'tx_id': tx_id,
                            'ts': ts,
                            'op_type': 'update',
                            'key': key,
                            'pkey': pkey_str,
                            'value': updated_value
                        })
                        # Update the value in pkey_info
                        pkey_info[pkey] = (key, pkey_str, updated_value)
                    elif op_type == 'delete':
                        commands.append({
                            'tx_id': tx_id,
                            'ts': ts,
                            'op_type': 'delete',
                            'key': key,
                            'pkey': pkey_str,
                            'value': ''
                        })
                        # Remove pkey from available pkeys
                        available_pkeys.remove(pkey)
                        del pkey_info[pkey]
                    elif op_type == 'get':
                        commands.append({
                            'tx_id': tx_id,
                            'ts': ts,
                            'op_type': 'get',
                            'key': key,
                            'pkey': pkey_str,
                            'value': ''
                        })
                    elif op_type == 'scan':
                        commands.append({
                            'tx_id': tx_id,
                            'ts': ts,
                            'op_type': 'scan',
                            'key': '',
                            'pkey': '',
                            'value': ''
                        })
        if commands:
            # Add commit command as the last operation
            commands.append({
                'tx_id': tx_id,
                'ts': ts,
                'op_type': 'commit',
                'key': '',
                'pkey': '',
                'value': ''
            })

            transactions.append({
                'tx_id': tx_id,
                'ts': ts,
                'commands': commands
            })

    # Write transactions to txs.csv
    with open(txs_file, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        for tx in transactions:
            for cmd in tx['commands']:
                writer.writerow([
                    cmd['tx_id'], cmd['ts'], cmd['op_type'], cmd['key'], cmd['pkey'], cmd['value']
                ])
            # Add an empty line between transactions for readability
            writer.writerow([])

    print(f"Generated {len(transactions)} transactions in {txs_file}")

    # Generate operations for ops.csv
    # Collect all commands into a single list and record commit operations
    all_operations = []
    op_id = 0
    tx_commit_op_id = {}  # Map tx_id to its commit operation ID
    for tx in transactions:
        for cmd in tx['commands']:
            cmd['id'] = op_id
            all_operations.append(cmd)
            if cmd['op_type'] == 'commit':
                tx_commit_op_id[tx['tx_id']] = op_id
            op_id += 1

    # Build dependency graph
    graph = defaultdict(set)
    in_degree = defaultdict(int)

    # Collect operations per pkey
    pkey_ops = defaultdict(list)
    for op in all_operations:
        pkey = op['pkey']
        if op['op_type'] != 'commit' and pkey:
            pkey_ops[pkey].append(op)

    # Enforce intra-transaction order (including commit)
    tx_ops = defaultdict(list)
    for op in all_operations:
        tx_ops[op['tx_id']].append(op)

    for tx_id, ops in tx_ops.items():
        ops.sort(key=lambda x: x['ts'])
        for i in range(len(ops) - 1):
            from_op = ops[i]['id']
            to_op = ops[i + 1]['id']
            if to_op not in graph[from_op]:
                graph[from_op].add(to_op)
                in_degree[to_op] += 1

    # Build mapping from tx_id to ts
    tx_ts = {tx['tx_id']: tx['ts'] for tx in transactions}

    # Collect pkeys touched by each transaction
    tx_pkeys = defaultdict(set)
    for op in all_operations:
        tx_id = op['tx_id']
        if op['op_type'] != 'commit' and op['pkey']:
            tx_pkeys[tx_id].add(op['pkey'])

    # Build pkey to list of transactions that touch it
    pkey_tx_list = defaultdict(list)
    for pkey, ops in pkey_ops.items():
        tx_ids = set()
        for op in ops:
            tx_id = op['tx_id']
            if tx_id not in tx_ids:
                tx_ids.add(tx_id)
                pkey_tx_list[pkey].append((tx_id, tx_ts[tx_id]))
        # Sort transactions by ts
        pkey_tx_list[pkey].sort(key=lambda x: x[1])

    # Build mapping of tx_id to pkey to ops
    tx_pkey_ops = defaultdict(lambda: defaultdict(list))
    for op in all_operations:
        tx_id = op['tx_id']
        pkey = op['pkey']
        if op['op_type'] != 'commit' and pkey:
            tx_pkey_ops[tx_id][pkey].append(op)

    # Enforce inter-transaction commit dependencies
    for pkey, tx_list in pkey_tx_list.items():
        for i in range(len(tx_list) - 1):
            tx_id_from = tx_list[i][0]
            tx_id_to = tx_list[i + 1][0]
            commit_op_id = tx_commit_op_id[tx_id_from]
            # For all operations in tx_id_to on this pkey, add dependency
            for op in tx_pkey_ops[tx_id_to][pkey]:
                if op['id'] not in graph[commit_op_id]:
                    graph[commit_op_id].add(op['id'])
                    in_degree[op['id']] += 1

    # Perform randomized topological sort
    zero_in_degree = [op['id'] for op in all_operations if in_degree[op['id']] == 0]
    random.shuffle(zero_in_degree)
    sorted_ops = []
    visited = set()

    while zero_in_degree:
        op_id = zero_in_degree.pop()
        if op_id in visited:
            continue
        op = all_operations[op_id]
        sorted_ops.append(op)
        visited.add(op_id)

        neighbors = list(graph[op_id])
        random.shuffle(neighbors)  # Shuffle to introduce randomness
        for neighbor in neighbors:
            in_degree[neighbor] -= 1
            if in_degree[neighbor] == 0:
                zero_in_degree.append(neighbor)
                random.shuffle(zero_in_degree)

    if len(sorted_ops) != len(all_operations):
        raise ValueError("Cycle detected in operations; cannot perform topological sort.")

    # Write mixed operations to ops.csv
    with open(ops_file, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        for op in sorted_ops:
            writer.writerow([
                op['tx_id'], op['ts'], op['op_type'], op['key'], op['pkey'], op['value']
            ])

    print(f"Generated mixed operations in {ops_file}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Generate transactions and mixed operations for hash join table.',
        formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument(
        '-df', '--data_file',
        default='./csv/data.csv',
        help='Input data file containing keys, pkeys, and values. (default: data.csv)'
    )
    parser.add_argument(
        '-tf', '--txs_file',
        default='./csv/txs.csv',
        help='Output transactions file to write generated transactions. (default: txs.csv)'
    )
    parser.add_argument(
        '-of', '--ops_file',
        default='./csv/ops.csv',
        help='Output operations file with mixed order. (default: ops.csv)'
    )
    parser.add_argument(
        '-n', '--num_transactions',
        type=int,
        default=10,
        help='Total number of transactions to generate. (default: 10)'
    )
    parser.add_argument(
        '-minc', '--min_cmds_per_tx',
        type=int,
        default=5,
        help='Minimum number of commands per transaction. (default: 5)'
    )
    parser.add_argument(
        '-maxc', '--max_cmds_per_tx',
        type=int,
        default=10,
        help='Maximum number of commands per transaction. (default: 10)'
    )
    parser.add_argument(
        '-ro', '--read_only_ratio',
        type=float,
        default=0.1,
        help=(
            'Ratio of read-only transactions.\n'
            'Specifies the fraction of transactions that are read-only (contain only get operations).\n'
            'Must be between 0 and 1. (default: 0.1)'
        )
    )
    parser.add_argument(
        '-i', '--insert_ratio',
        type=float,
        default=0.3,
        help='Ratio of insert operations in read-write transactions. (default: 0.3)'
    )
    parser.add_argument(
        '-u', '--update_ratio',
        type=float,
        default=0.3,
        help='Ratio of update operations in read-write transactions. (default: 0.3)'
    )
    parser.add_argument(
        '-d', '--delete_ratio',
        type=float,
        default=0.2,
        help='Ratio of delete operations in read-write transactions. (default: 0.2)'
    )
    parser.add_argument(
        '-g', '--get_ratio',
        type=float,
        default=0.1,
        help='Ratio of get operations in read-write transactions. (default: 0.1)'
    )
    parser.add_argument(
        '-s', '--scan_ratio',
        type=float,
        default=0.1,
        help='Ratio of scan operations in read-write transactions. (default: 0.1)'
    )
    args = parser.parse_args()
    
    # Validate ratios
    total_ratio = args.insert_ratio + args.update_ratio + args.delete_ratio + args.get_ratio + args.scan_ratio
    if abs(total_ratio - 1.0) > 0.001:
        print("Error: Insert, update, delete, get, and scan ratios must sum to 1.0")
        exit(1)

    generate_transactions_and_operations(
        data_file=args.data_file,
        txs_file=args.txs_file,
        ops_file=args.ops_file,
        num_transactions=args.num_transactions,
        min_cmds_per_tx=args.min_cmds_per_tx,
        max_cmds_per_tx=args.max_cmds_per_tx,
        read_only_ratio=args.read_only_ratio,
        insert_ratio=args.insert_ratio,
        update_ratio=args.update_ratio,
        delete_ratio=args.delete_ratio,
        get_ratio=args.get_ratio,
        scan_ratio=args.scan_ratio
    )
