#!/usr/bin/env python3
"""
Generate update operations and resulting PART table.

Usage:
    python generate_updates.py <part.tbl> <update_percent> <sf> [--zipf THETA] [--seed SEED]

Examples:
    python generate_updates.py tpch_data/part_sf0.1.tbl 1 0.1
    python generate_updates.py tpch_data/part_sf0.1.tbl 1 0.1 --zipf 0.99
    python generate_updates.py tpch_data/part_sf0.1.tbl 10 0.1 --zipf 0.5

Outputs:
    part_updates_sf<sf>_<pct>pct_uniform.tbl   (or _zipf<theta>.tbl)
    part_updated_sf<sf>_<pct>pct_uniform.tbl   (or _zipf<theta>.tbl)

Update file: sequence of update operations (same key can appear multiple times).
Updated file: final PART table after all updates applied in order.
"""

import argparse
import math
import random
import os
import sys


# All 150 valid P_TYPE values from TPC-H spec
PREFIXES = ["STANDARD", "SMALL", "MEDIUM", "LARGE", "ECONOMY", "PROMO"]
SYLLABLES = ["ANODIZED", "BRUSHED", "BURNISHED", "PLATED", "POLISHED"]
MATERIALS = ["TIN", "NICKEL", "BRASS", "STEEL", "COPPER"]
ALL_TYPES = [f"{p} {s} {m}" for p in PREFIXES for s in SYLLABLES for m in MATERIALS]


class ZipfGenerator:
    """Zipfian distribution generator (same as YCSB)."""

    def __init__(self, n, theta, seed=42):
        self.n = n
        self.theta = theta
        self.rng = random.Random(seed)
        # Precompute harmonic numbers
        self.zeta_n = self._zeta(n)
        self.zeta_2 = self._zeta(2)
        self.alpha = 1.0 / (1.0 - theta)
        self.eta = (1.0 - (2.0 / n) ** (1.0 - theta)) / (1.0 - self.zeta_2 / self.zeta_n)

    def _zeta(self, n):
        s = 0.0
        for i in range(1, n + 1):
            s += 1.0 / (i ** self.theta)
        return s

    def next(self):
        u = self.rng.random()
        uz = u * self.zeta_n
        if uz < 1.0:
            return 0
        if uz < 1.0 + 0.5 ** self.theta:
            return 1
        return int(self.n * ((self.eta * u - self.eta + 1.0) ** self.alpha))


def main():
    parser = argparse.ArgumentParser(description="Generate PART updates for hash table testing")
    parser.add_argument("part_tbl", help="Path to PART.tbl")
    parser.add_argument("update_percent", type=float, help="Percentage of rows to generate as update ops (0-100)")
    parser.add_argument("sf", type=str, help="Scale factor (used in output filenames)")
    parser.add_argument("--zipf", type=float, default=None,
                        help="Zipfian theta (0=uniform, ~1=very skewed). YCSB default is 0.99. "
                             "If not set, uses uniform distribution.")
    parser.add_argument("--seed", type=int, default=42, help="Random seed (default: 42)")
    parser.add_argument("--output-dir", default=None, help="Output directory (default: same as input)")
    args = parser.parse_args()

    if not 0 < args.update_percent <= 100:
        print("Error: update_percent must be between 0 and 100", file=sys.stderr)
        sys.exit(1)

    random.seed(args.seed)

    with open(args.part_tbl, "r") as f:
        lines = f.readlines()

    num_rows = len(lines)
    num_ops = int(num_rows * args.update_percent / 100)

    # Build distribution label for filenames
    pct_str = f"{args.update_percent:g}"
    if args.zipf is not None:
        dist_label = f"zipf{args.zipf:g}"
    else:
        dist_label = "uniform"

    out_dir = args.output_dir or os.path.dirname(args.part_tbl) or "."
    base = f"part_updates_sf{args.sf}_{pct_str}pct_{dist_label}"
    updates_path = os.path.join(out_dir, f"{base}.tbl")
    updated_base = f"part_updated_sf{args.sf}_{pct_str}pct_{dist_label}"
    updated_path = os.path.join(out_dir, f"{updated_base}.tbl")

    # Generate update target indices
    if args.zipf is not None:
        zipf = ZipfGenerator(num_rows, args.zipf, seed=args.seed)
        target_indices = [zipf.next() for _ in range(num_ops)]
    else:
        target_indices = [random.randint(0, num_rows - 1) for _ in range(num_ops)]

    # Parse all rows
    rows = [line.rstrip("\n").split("|") for line in lines]

    # Generate update operations in order
    update_ops = []
    for idx in target_indices:
        fields = list(rows[idx])  # copy current state
        old_type = fields[4]
        new_type = old_type
        while new_type == old_type:
            new_type = random.choice(ALL_TYPES)
        fields[4] = new_type
        rows[idx] = fields  # apply update
        update_ops.append("|".join(fields) + "\n")

    # Write update operations
    with open(updates_path, "w") as f:
        f.writelines(update_ops)

    # Write final table
    with open(updated_path, "w") as f:
        for row in rows:
            f.write("|".join(row) + "\n")

    # Stats
    unique_keys = len(set(target_indices))
    print(f"Total rows:      {num_rows}")
    print(f"Update ops:      {num_ops} ({pct_str}%)")
    print(f"Unique keys hit: {unique_keys}")
    print(f"Distribution:    {dist_label}")
    print(f"Updates file:    {updates_path}")
    print(f"Updated table:   {updated_path}")


if __name__ == "__main__":
    main()
