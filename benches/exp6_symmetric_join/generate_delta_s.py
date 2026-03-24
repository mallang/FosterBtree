#!/usr/bin/env python3
"""Split a LINEITEM probe file into initial batch + delta batches for symmetric join.

Usage:
    python generate_delta_s.py <lineitem_file> <output_dir> [--initial_pct 80] [--rounds 5]

Output:
    <output_dir>/lineitem_initial.tbl   (first initial_pct% of rows)
    <output_dir>/lineitem_delta.tbl     (remaining rows, used as delta stream)
"""

import argparse
import os
import random

def main():
    parser = argparse.ArgumentParser(description='Split LINEITEM into initial + delta batches')
    parser.add_argument('lineitem_file', help='Input LINEITEM probe file')
    parser.add_argument('output_dir', help='Output directory')
    parser.add_argument('--initial_pct', type=float, default=80.0,
                        help='Percentage of rows for initial load (default: 80)')
    parser.add_argument('--seed', type=int, default=42, help='Random seed')
    args = parser.parse_args()

    os.makedirs(args.output_dir, exist_ok=True)

    # Read all lines
    with open(args.lineitem_file, 'r') as f:
        lines = [l for l in f if l.strip()]

    total = len(lines)
    initial_count = int(total * args.initial_pct / 100.0)

    # Shuffle with seed for reproducibility
    random.seed(args.seed)
    indices = list(range(total))
    random.shuffle(indices)

    initial_indices = sorted(indices[:initial_count])
    delta_indices = sorted(indices[initial_count:])

    # Write initial
    initial_file = os.path.join(args.output_dir, 'lineitem_initial.tbl')
    with open(initial_file, 'w') as f:
        for i in initial_indices:
            f.write(lines[i])
    print(f'Initial: {len(initial_indices)} rows -> {initial_file}')

    # Write delta (all remaining as one file; symmetric_bench splits into rounds)
    delta_file = os.path.join(args.output_dir, 'lineitem_delta.tbl')
    with open(delta_file, 'w') as f:
        for i in delta_indices:
            f.write(lines[i])
    print(f'Delta:   {len(delta_indices)} rows -> {delta_file}')


if __name__ == '__main__':
    main()
