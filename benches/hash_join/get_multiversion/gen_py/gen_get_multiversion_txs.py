import csv
import argparse
import random
from collections import defaultdict
import string

def generate_transactions_and_operations(
    data_file_prefix,
    txs_file,
    ops_file,
    num_transactions,
    min_cmds_per_tx,
    max_cmds_per_tx,
    history_get_ratio,
    partitions,
):
    # Load data and determine pkey length
    data = {}
    pkey_lengths = set()
    value_lengths = []  # To collect lengths of values
    max_init_ts = 0
    # remove extension of data_file_prefix
    data_file_prefix = data_file_prefix.rsplit('.', 1)[0]
    csv_dir = data_file_prefix.rsplit('/', 1)[0]
    data_file_prefix = data_file_prefix.rsplit('/', 1)[-1]
    # find all files with prefix in csv_dir
    data_files = [f"{csv_dir}/{data_file_prefix}_partition_{i}.csv" for i in range(0, partitions)]  # Example: data_0.csv, data_1.csv, ...
    for data_file in data_files:
        with open(data_file, 'r') as csvfile:
            reader = csv.reader(csvfile)
            for row in reader:
                if len(row) < 6:
                    continue  # Skip invalid rows
                tx_id, _start_ts, op, key, pkey_str, value = row
                start_ts = int(_start_ts)
                if op == "insert":
                    data[pkey_str] = [key, pkey_str, {start_ts: value}]
                elif op == "update":
                    data[pkey_str][2][start_ts] = value
                else:
                    print("Warning, unknown op {op}")
                pkey_lengths.add(len(pkey_str))
                value_lengths.append(len(value))  # Collect value lengths
                max_init_ts = max(max_init_ts, start_ts)

    # Ensure pkey lengths are consistent
    if len(pkey_lengths) != 1:
        raise ValueError("Inconsistent pkey lengths in data file.")


    # Prepare for generating new pkeys
    existing_pkeys = set(data.keys())


    # Initialize the pool of pkeys for operations
    available_pkeys = set(existing_pkeys)
    pkey_info = {}  # Maps pkey to (key, pkey, value)

    for key, pkey, value in data.values():
        pkey_info[pkey] = (key, pkey, value)

    # Generate transactions
    transactions = []
    tx_id = max_init_ts  # Start from max_init_ts for consistency with op IDs
    ts = max_init_ts
    for _ in range(num_transactions):
        tx_id += 1
        ts += 1
        num_cmds = random.randint(min_cmds_per_tx, max_cmds_per_tx)
        commands = []
        for _ in range(num_cmds):
            if not available_pkeys:
                continue  # No available pkeys to read
            pkey = random.choice(list(available_pkeys))
            key, pkey_str, version_dict = pkey_info[pkey]
            
            is_history_get = random.random() < history_get_ratio

            recent_version_ts = max(version_dict.keys())
            if is_history_get :
                history_versions = {k: v for k, v in version_dict.items() if k != recent_version_ts}
                history_ts = random.choice(list(history_versions.keys()))
                commands.append({
                    'tx_id': tx_id,
                    'ts': int(history_ts),
                    'op_type': 'get',
                    'key': key,
                    'pkey': pkey_str,
                    'value': '',
                })
            else :
                # recent get
                commands.append({
                    'tx_id': tx_id,
                    'ts': int(recent_version_ts),
                    'op_type': 'get',
                    'key': key,
                    'pkey': pkey_str,
                    'value': '',
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

    # # Write transactions to txs.csv
    # with open(txs_file, 'w', newline='') as csvfile:
    #     writer = csv.writer(csvfile)
    #     for tx in transactions:
    #         for cmd in tx['commands']:
    #             writer.writerow([
    #                 cmd['tx_id'], cmd['ts'], cmd['op_type'], cmd['key'], cmd['pkey'], cmd['value']
    #             ])
    #         # Add an empty line between transactions for readability
    #         writer.writerow([])

    # print(f"Generated {len(transactions)} transactions in {txs_file}, with history_get_ratio: {history_get_ratio}")

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

    # # Build dependency graph
    # graph = defaultdict(set)
    # in_degree = defaultdict(int)

    # # Collect operations per pkey
    # pkey_ops = defaultdict(list)
    # for op in all_operations:
    #     pkey = op['pkey']
    #     if op['op_type'] != 'commit' and pkey:
    #         pkey_ops[pkey].append(op)

    # # Enforce intra-transaction order (including commit)
    # tx_ops = defaultdict(list)
    # for op in all_operations:
    #     tx_ops[op['tx_id']].append(op)

    # for tx_id, ops in tx_ops.items():
    #     ops.sort(key=lambda x: x['ts'])
    #     for i in range(len(ops) - 1):
    #         from_op = ops[i]['id']
    #         to_op = ops[i + 1]['id']
    #         if to_op not in graph[from_op]:
    #             graph[from_op].add(to_op)
    #             in_degree[to_op] += 1

    # # Build mapping from tx_id to ts
    # tx_ts = {tx['tx_id']: tx['ts'] for tx in transactions}

    # # Collect pkeys touched by each transaction
    # tx_pkeys = defaultdict(set)
    # for op in all_operations:
    #     tx_id = op['tx_id']
    #     if op['op_type'] != 'commit' and op['pkey']:
    #         tx_pkeys[tx_id].add(op['pkey'])

    # # Build pkey to list of transactions that touch it
    # pkey_tx_list = defaultdict(list)
    # for pkey, ops in pkey_ops.items():
    #     tx_ids = set()
    #     for op in ops:
    #         tx_id = op['tx_id']
    #         if tx_id not in tx_ids:
    #             tx_ids.add(tx_id)
    #             pkey_tx_list[pkey].append((tx_id, tx_ts[tx_id]))
    #     # Sort transactions by ts
    #     pkey_tx_list[pkey].sort(key=lambda x: x[1])

    # # Build mapping of tx_id to pkey to ops
    # tx_pkey_ops = defaultdict(lambda: defaultdict(list))
    # for op in all_operations:
    #     tx_id = op['tx_id']
    #     pkey = op['pkey']
    #     if op['op_type'] != 'commit' and pkey:
    #         tx_pkey_ops[tx_id][pkey].append(op)

    # # Enforce inter-transaction commit dependencies
    # for pkey, tx_list in pkey_tx_list.items():
    #     for i in range(len(tx_list) - 1):
    #         tx_id_from = tx_list[i][0]
    #         tx_id_to = tx_list[i + 1][0]
    #         commit_op_id = tx_commit_op_id[tx_id_from]
    #         # For all operations in tx_id_to on this pkey, add dependency
    #         for op in tx_pkey_ops[tx_id_to][pkey]:
    #             if op['id'] not in graph[commit_op_id]:
    #                 graph[commit_op_id].add(op['id'])
    #                 in_degree[op['id']] += 1

    # # Perform randomized topological sort
    # zero_in_degree = [op['id'] for op in all_operations if in_degree[op['id']] == 0]
    # random.shuffle(zero_in_degree)
    # sorted_ops = []
    # visited = set()

    # while zero_in_degree:
    #     op_id = zero_in_degree.pop()
    #     if op_id in visited:
    #         continue
    #     op = all_operations[op_id]
    #     sorted_ops.append(op)
    #     visited.add(op_id)

    #     neighbors = list(graph[op_id])
    #     random.shuffle(neighbors)  # Shuffle to introduce randomness
    #     for neighbor in neighbors:
    #         in_degree[neighbor] -= 1
    #         if in_degree[neighbor] == 0:
    #             zero_in_degree.append(neighbor)
    #             random.shuffle(zero_in_degree)

    # if len(sorted_ops) != len(all_operations):
    #     raise ValueError("Cycle detected in operations; cannot perform topological sort.")

    # Write mixed operations to ops.csv
    with open(ops_file, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        for op in all_operations:
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
        '-hgr', '--history_get_ratio',
        type=float,
        default=0.0,
        help='Ratio of get history version operations in read transactions. (default: 0.0)'
    )
    parser.add_argument(
        '-par', '--partition_number',
        type=int,
        default=1,
        help='partition number (default: 1)'
    )
    args = parser.parse_args()

    if (args.history_get_ratio - 0.0) < -0.001 or (args.history_get_ratio - 1.0) > 0.001 :
        print("Error: history get ratio should withio [0.0, 1.0]")
        exit(1)
    
    generate_transactions_and_operations(
        data_file_prefix=args.data_file,
        txs_file=args.txs_file,
        ops_file=args.ops_file,
        num_transactions=args.num_transactions,
        min_cmds_per_tx=args.min_cmds_per_tx,
        max_cmds_per_tx=args.max_cmds_per_tx,
        history_get_ratio=args.history_get_ratio,
        partitions=args.partition_number,
    )
