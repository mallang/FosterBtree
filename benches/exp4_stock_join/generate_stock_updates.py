#!/usr/bin/env python3
"""
Generate skewed (Zipfian) update operations for the STOCK table.

Usage:
    python generate_stock_updates.py <stock.tbl> <update_percent> <warehouses>
        [--zipf THETA] [--seed SEED] [--output-dir DIR]

Examples:
    python generate_stock_updates.py data/stock_w1.tbl 5  1              # 5%  uniform
    python generate_stock_updates.py data/stock_w1.tbl 10 1 --zipf 0.99 # 10% zipf
    python generate_stock_updates.py data/stock_w1.tbl 20 1 --zipf 0.99 # 20% zipf

Outputs:
    stock_updates_w<W>_<pct>pct_<dist>.tbl   -- sequence of update ops (S_I_ID changes S_QUANTITY)
    stock_updated_w<W>_<pct>pct_<dist>.tbl   -- final STOCK state after all updates applied

Update format: same pipe-separated STOCK row format, with new S_QUANTITY at field[2].
Each row = one update operation at a distinct timestamp.
"""

import argparse
import os
import random
import sys


class ZipfGenerator:
    """Zipfian distribution generator (same implementation as generate_updates.py)."""

    def __init__(self, n: int, theta: float, seed: int = 42):
        self.n = n
        self.theta = theta
        self.rng = random.Random(seed)
        self.zeta_n = self._zeta(n)
        self.zeta_2 = self._zeta(2)
        self.alpha = 1.0 / (1.0 - theta)
        self.eta = (1.0 - (2.0 / n) ** (1.0 - theta)) / (1.0 - self.zeta_2 / self.zeta_n)

    def _zeta(self, n: int) -> float:
        s = 0.0
        for i in range(1, n + 1):
            s += 1.0 / (i ** self.theta)
        return s

    def next(self) -> int:
        u = self.rng.random()
        uz = u * self.zeta_n
        if uz < 1.0:
            return 0
        if uz < 1.0 + 0.5 ** self.theta:
            return 1
        return int(self.n * ((self.eta * u - self.eta + 1.0) ** self.alpha))


def new_quantity(rng: random.Random, old_qty: int) -> int:
    """Generate a new S_QUANTITY different from the current value."""
    new_qty = old_qty
    while new_qty == old_qty:
        new_qty = rng.randint(10, 100)
    return new_qty


def main():
    parser = argparse.ArgumentParser(
        description="Generate skewed STOCK updates for Experiment 4"
    )
    parser.add_argument("stock_tbl", help="Path to STOCK.tbl")
    parser.add_argument("update_percent", type=float,
                        help="Percentage of rows to generate as update ops (0-100)")
    parser.add_argument("warehouses", type=str,
                        help="Warehouse count label (used in output filenames)")
    parser.add_argument("--zipf", type=float, default=None,
                        help="Zipfian theta (e.g. 0.99). If not set, uses uniform.")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--output-dir", default=None,
                        help="Output directory (default: same dir as input)")
    args = parser.parse_args()

    if not 0 <= args.update_percent <= 100:
        print("Error: update_percent must be between 0 and 100", file=sys.stderr)
        sys.exit(1)

    rng = random.Random(args.seed)

    with open(args.stock_tbl, "r") as f:
        lines = f.readlines()

    num_rows = len(lines)
    num_ops = int(num_rows * args.update_percent / 100)

    pct_str = f"{args.update_percent:g}"
    if args.zipf is not None:
        dist_label = f"zipf{args.zipf:g}"
    else:
        dist_label = "uniform"

    out_dir = args.output_dir or os.path.dirname(args.stock_tbl) or "."
    W = args.warehouses

    updates_path = os.path.join(out_dir, f"stock_updates_w{W}_{pct_str}pct_{dist_label}.tbl")
    updated_path = os.path.join(out_dir, f"stock_updated_w{W}_{pct_str}pct_{dist_label}.tbl")

    # Parse all rows; track mutable S_QUANTITY per row
    rows = [line.rstrip("\n").split("|") for line in lines]
    # S_QUANTITY is field[2]
    quantities = [int(row[2]) for row in rows]

    # Generate update target indices
    if num_ops == 0:
        target_indices = []
    elif args.zipf is not None:
        zipf = ZipfGenerator(num_rows, args.zipf, seed=args.seed)
        target_indices = [zipf.next() % num_rows for _ in range(num_ops)]
    else:
        target_indices = [rng.randint(0, num_rows - 1) for _ in range(num_ops)]

    # Generate update operations in order
    update_ops = []
    for idx in target_indices:
        fields = list(rows[idx])
        old_qty = quantities[idx]
        new_qty = new_quantity(rng, old_qty)
        fields[2] = str(new_qty)
        quantities[idx] = new_qty       # track current state
        rows[idx] = fields              # apply update for final state
        update_ops.append("|".join(fields) + "\n")

    # Write update operations file (sequence of ops, one per line)
    with open(updates_path, "w") as f:
        f.writelines(update_ops)

    # Write final STOCK table state
    with open(updated_path, "w") as f:
        for row in rows:
            f.write("|".join(row) + "\n")

    # Stats
    unique_keys = len(set(target_indices)) if target_indices else 0
    print(f"Total rows:      {num_rows:,}")
    print(f"Update ops:      {num_ops:,} ({pct_str}%)")
    print(f"Unique keys hit: {unique_keys:,}")
    print(f"Distribution:    {dist_label}")
    print(f"Updates file:    {updates_path}")
    print(f"Updated table:   {updated_path}")


if __name__ == "__main__":
    main()
