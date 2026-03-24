#!/usr/bin/env python3
"""
Generate CH-benCHmark STOCK and ORDER_LINE tables (TPC-C compatible).

Usage:
    python generate_chbenchmark_data.py [--warehouses W] [--output-dir DIR] [--seed SEED]

Outputs (pipe-separated, no header):
    stock_w<W>.tbl
        S_I_ID|S_W_ID|S_QUANTITY|S_DIST_01|...|S_DIST_10|S_YTD|S_ORDER_CNT|S_REMOTE_CNT|S_DATA
    orderline_w<W>.tbl
        OL_O_ID|OL_D_ID|OL_W_ID|OL_NUMBER|OL_I_ID|OL_SUPPLY_W_ID|OL_DELIVERY_D|OL_QUANTITY|OL_AMOUNT|OL_DIST_INFO

Scale (TPC-C spec):
    - STOCK:      100,000 rows per warehouse
    - ORDER_LINE: ~300,000 rows per warehouse
                  (10 districts x 3000 orders x avg 10 order lines)
"""

import argparse
import os
import random
import string
import sys


# ── TPC-C constants ──────────────────────────────────────────────────────────
ITEMS_PER_WAREHOUSE = 100_000
DISTRICTS_PER_WAREHOUSE = 10
ORDERS_PER_DISTRICT = 3000
OL_MIN = 5
OL_MAX = 15
DIST_STRING_LEN = 24  # S_DIST_xx / OL_DIST_INFO length
DATA_MIN_LEN = 26
DATA_MAX_LEN = 50


def rand_alpha(length: int, rng: random.Random) -> str:
    return ''.join(rng.choices(string.ascii_uppercase + string.digits, k=length))


def rand_astring(min_len: int, max_len: int, rng: random.Random) -> str:
    n = rng.randint(min_len, max_len)
    return rand_alpha(n, rng)


def generate_stock(warehouse_id: int, rng: random.Random) -> list[str]:
    """Generate STOCK rows for one warehouse."""
    rows = []
    for i_id in range(1, ITEMS_PER_WAREHOUSE + 1):
        quantity = rng.randint(10, 100)
        dists = [rand_alpha(DIST_STRING_LEN, rng) for _ in range(10)]
        ytd = 0
        order_cnt = 0
        remote_cnt = 0
        data = rand_astring(DATA_MIN_LEN, DATA_MAX_LEN, rng)

        row = (
            f"{i_id}|{warehouse_id}|{quantity}|"
            + "|".join(dists)
            + f"|{ytd}|{order_cnt}|{remote_cnt}|{data}"
        )
        rows.append(row + "\n")
    return rows


def generate_orderline(warehouse_id: int, rng: random.Random) -> list[str]:
    """Generate ORDER_LINE rows for one warehouse following TPC-C spec."""
    rows = []
    for d_id in range(1, DISTRICTS_PER_WAREHOUSE + 1):
        for o_id in range(1, ORDERS_PER_DISTRICT + 1):
            ol_cnt = rng.randint(OL_MIN, OL_MAX)
            for ol_number in range(1, ol_cnt + 1):
                ol_i_id = rng.randint(1, ITEMS_PER_WAREHOUSE)
                ol_supply_w_id = warehouse_id
                # Last 900 orders per district have null delivery date (new orders)
                if o_id <= ORDERS_PER_DISTRICT - 900:
                    ol_delivery_d = "2006-01-01 00:00:00"
                    ol_amount = 0
                else:
                    ol_delivery_d = "NULL"
                    ol_amount = round(rng.uniform(0.01, 9999.99), 2)
                ol_quantity = 5
                ol_dist_info = rand_alpha(DIST_STRING_LEN, rng)

                row = (
                    f"{o_id}|{d_id}|{warehouse_id}|{ol_number}|{ol_i_id}|"
                    f"{ol_supply_w_id}|{ol_delivery_d}|{ol_quantity}|"
                    f"{ol_amount}|{ol_dist_info}"
                )
                rows.append(row + "\n")
    return rows


def main():
    parser = argparse.ArgumentParser(
        description="Generate CH-benCHmark STOCK and ORDER_LINE tables"
    )
    parser.add_argument("--warehouses", type=int, default=1,
                        help="Number of warehouses (default: 1)")
    parser.add_argument("--output-dir", default=None,
                        help="Output directory (default: ./data next to this script)")
    parser.add_argument("--seed", type=int, default=42,
                        help="Random seed (default: 42)")
    args = parser.parse_args()

    W = args.warehouses
    rng = random.Random(args.seed)

    script_dir = os.path.dirname(os.path.abspath(__file__))
    out_dir = args.output_dir or os.path.join(script_dir, "data")
    os.makedirs(out_dir, exist_ok=True)

    stock_path = os.path.join(out_dir, f"stock_w{W}.tbl")
    orderline_path = os.path.join(out_dir, f"orderline_w{W}.tbl")

    print(f"Generating STOCK (W={W}, {W * ITEMS_PER_WAREHOUSE:,} rows) ...")
    stock_rows = []
    for w in range(1, W + 1):
        stock_rows.extend(generate_stock(w, rng))

    with open(stock_path, "w") as f:
        f.writelines(stock_rows)

    print(f"  -> {stock_path}  ({len(stock_rows):,} rows)")

    print(f"Generating ORDER_LINE (W={W}, ~{W * DISTRICTS_PER_WAREHOUSE * ORDERS_PER_DISTRICT * 10:,} rows) ...")
    orderline_rows = []
    for w in range(1, W + 1):
        orderline_rows.extend(generate_orderline(w, rng))

    with open(orderline_path, "w") as f:
        f.writelines(orderline_rows)

    print(f"  -> {orderline_path}  ({len(orderline_rows):,} rows)")
    print("Done.")


if __name__ == "__main__":
    main()
