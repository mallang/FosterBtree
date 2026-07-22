#!/usr/bin/env python3
import argparse
import csv
import math
from collections import defaultdict
from pathlib import Path


METRICS = [
    ("throughput_ops_sec", "Throughput", "ops/sec"),
    ("avg_latency_ns", "Average latency", "ns"),
    ("p50_latency_ns", "P50 latency", "ns"),
    ("p95_latency_ns", "P95 latency", "ns"),
    ("p99_latency_ns", "P99 latency", "ns"),
]

VARIANT_COLORS = {
    "paged_hash_chain": "#7a7a7a",
    "paged_hash_chain_v1": "#1f77b4",
    "hash_btree": "#d62728",
}

WIDTH = 920
HEIGHT = 560
MARGIN_LEFT = 92
MARGIN_RIGHT = 34
MARGIN_TOP = 62
MARGIN_BOTTOM = 82
PLOT_WIDTH = WIDTH - MARGIN_LEFT - MARGIN_RIGHT
PLOT_HEIGHT = HEIGHT - MARGIN_TOP - MARGIN_BOTTOM


def main():
    parser = argparse.ArgumentParser(
        description="Plot hash_vs_tree_v0 CSV output as dependency-free SVG charts."
    )
    parser.add_argument("--input", required=True, nargs="+", type=Path)
    parser.add_argument("--out-dir", required=True, type=Path)
    parser.add_argument("--x", default="threads", choices=["threads", "n_keys", "bucket_count"])
    parser.add_argument(
        "--variant",
        action="append",
        dest="variants",
        help="Variant to include. Can be repeated. Defaults to all variants in the CSV.",
    )
    args = parser.parse_args()

    rows = []
    for input_path in args.input:
        rows.extend(read_rows(input_path))
    if args.variants:
        wanted = set(args.variants)
        rows = [row for row in rows if row["variant"] in wanted]
    if not rows:
        raise SystemExit("no rows to plot")

    args.out_dir.mkdir(parents=True, exist_ok=True)

    groups = defaultdict(list)
    for row in rows:
        groups[(row["distribution"], row["zipf_theta"])].append(row)

    written = []
    for (distribution, theta), group_rows in sorted(groups.items()):
        for metric, title, unit in METRICS:
            if metric not in group_rows[0]:
                continue
            svg = render_line_chart(
                group_rows,
                x_col=args.x,
                y_col=metric,
                title=f"{title}: {distribution}, theta={theta:.3f}",
                y_label=unit,
            )
            stem = f"{metric}_by_{args.x}_{distribution}_theta_{theta:.3f}".replace(".", "p")
            out_path = args.out_dir / f"{stem}.svg"
            out_path.write_text(svg, encoding="utf-8")
            written.append(out_path)

    print("wrote")
    for path in written:
        print(path)


def read_rows(path):
    rows = []
    with path.open(newline="", encoding="utf-8") as file:
        reader = csv.DictReader(line for line in file if line and not line.startswith("building "))
        for raw in reader:
            if not raw or not raw.get("variant"):
                continue
            row = dict(raw)
            row["n_keys"] = int(row["n_keys"])
            row["key_size"] = int(row["key_size"])
            row["page_size"] = int(row["page_size"])
            row["zipf_theta"] = float(row["zipf_theta"])
            row["threads"] = int(row["threads"])
            row["measured_lookups"] = int(row["measured_lookups"])
            row["build_sec"] = float(row["build_sec"])
            row["duration_sec"] = float(row["duration_sec"])
            row["throughput_ops_sec"] = float(row["throughput_ops_sec"])
            row["avg_latency_ns"] = float(row["avg_latency_ns"])
            row["p50_latency_ns"] = float(row["p50_latency_ns"])
            row["p95_latency_ns"] = float(row["p95_latency_ns"])
            row["p99_latency_ns"] = float(row["p99_latency_ns"])
            row["bucket_count"] = int(row["bucket_count"])
            rows.append(row)
    return rows


def render_line_chart(rows, x_col, y_col, title, y_label):
    series = defaultdict(list)
    for row in rows:
        series[row["variant"]].append((float(row[x_col]), float(row[y_col])))

    for values in series.values():
        values.sort()

    x_values = sorted({x for values in series.values() for x, _ in values})
    y_values = [y for values in series.values() for _, y in values]
    x_min, x_max = min(x_values), max(x_values)
    y_min, y_max = 0.0, max(y_values)
    if x_min == x_max:
        x_min -= 0.5
        x_max += 0.5
    if y_max <= 0:
        y_max = 1.0
    y_max = nice_upper_bound(y_max)

    def sx(x):
        return MARGIN_LEFT + (x - x_min) / (x_max - x_min) * PLOT_WIDTH

    def sy(y):
        return MARGIN_TOP + (1.0 - (y - y_min) / (y_max - y_min)) * PLOT_HEIGHT

    parts = [
        '<svg xmlns="http://www.w3.org/2000/svg" '
        f'width="{WIDTH}" height="{HEIGHT}" viewBox="0 0 {WIDTH} {HEIGHT}">',
        "<style>",
        "text{font-family:Arial,Helvetica,sans-serif;fill:#222}",
        ".title{font-size:22px;font-weight:700}",
        ".axis{stroke:#333;stroke-width:1.2}",
        ".grid{stroke:#ddd;stroke-width:1}",
        ".tick{font-size:12px;fill:#555}",
        ".label{font-size:14px;fill:#333}",
        ".legend{font-size:13px;fill:#222}",
        "</style>",
        '<rect width="100%" height="100%" fill="#fff"/>',
        f'<text class="title" x="{MARGIN_LEFT}" y="34">{escape(title)}</text>',
    ]

    y_ticks = make_ticks(0.0, y_max, 5)
    for tick in y_ticks:
        y = sy(tick)
        parts.append(
            f'<line class="grid" x1="{MARGIN_LEFT}" y1="{y:.1f}" '
            f'x2="{WIDTH - MARGIN_RIGHT}" y2="{y:.1f}"/>'
        )
        parts.append(
            f'<text class="tick" x="{MARGIN_LEFT - 10}" y="{y + 4:.1f}" '
            f'text-anchor="end">{format_number(tick)}</text>'
        )

    for x in x_values:
        px = sx(x)
        parts.append(
            f'<line class="grid" x1="{px:.1f}" y1="{MARGIN_TOP}" '
            f'x2="{px:.1f}" y2="{HEIGHT - MARGIN_BOTTOM}"/>'
        )
        parts.append(
            f'<text class="tick" x="{px:.1f}" y="{HEIGHT - MARGIN_BOTTOM + 24}" '
            f'text-anchor="middle">{format_number(x)}</text>'
        )

    parts.extend(
        [
            f'<line class="axis" x1="{MARGIN_LEFT}" y1="{HEIGHT - MARGIN_BOTTOM}" '
            f'x2="{WIDTH - MARGIN_RIGHT}" y2="{HEIGHT - MARGIN_BOTTOM}"/>',
            f'<line class="axis" x1="{MARGIN_LEFT}" y1="{MARGIN_TOP}" '
            f'x2="{MARGIN_LEFT}" y2="{HEIGHT - MARGIN_BOTTOM}"/>',
            f'<text class="label" x="{MARGIN_LEFT + PLOT_WIDTH / 2}" '
            f'y="{HEIGHT - 28}" text-anchor="middle">{escape(x_col)}</text>',
            f'<text class="label" transform="translate(24 {MARGIN_TOP + PLOT_HEIGHT / 2}) '
            f'rotate(-90)" text-anchor="middle">{escape(y_label)}</text>',
        ]
    )

    legend_x = WIDTH - MARGIN_RIGHT - 225
    legend_y = MARGIN_TOP - 28
    for i, (variant, values) in enumerate(sorted(series.items())):
        color = VARIANT_COLORS.get(variant, fallback_color(i))
        points = " ".join(f"{sx(x):.1f},{sy(y):.1f}" for x, y in values)
        parts.append(
            f'<polyline fill="none" stroke="{color}" stroke-width="2.5" '
            f'stroke-linejoin="round" stroke-linecap="round" points="{points}"/>'
        )
        for x, y in values:
            parts.append(
                f'<circle cx="{sx(x):.1f}" cy="{sy(y):.1f}" r="4" fill="{color}"/>'
            )

        ly = legend_y + i * 22
        parts.append(f'<line x1="{legend_x}" y1="{ly}" x2="{legend_x + 24}" y2="{ly}" stroke="{color}" stroke-width="3"/>')
        parts.append(f'<text class="legend" x="{legend_x + 32}" y="{ly + 4}">{escape(variant)}</text>')

    parts.append("</svg>")
    return "\n".join(parts)


def make_ticks(min_value, max_value, count):
    if count <= 1:
        return [min_value, max_value]
    step = (max_value - min_value) / (count - 1)
    return [min_value + step * i for i in range(count)]


def nice_upper_bound(value):
    if value <= 0:
        return 1.0
    power = 10 ** math.floor(math.log10(value))
    scaled = value / power
    if scaled <= 1:
        nice = 1
    elif scaled <= 2:
        nice = 2
    elif scaled <= 5:
        nice = 5
    else:
        nice = 10
    return nice * power


def format_number(value):
    if abs(value) >= 1_000_000:
        return f"{value / 1_000_000:.1f}M"
    if abs(value) >= 1_000:
        return f"{value / 1_000:.1f}K"
    if float(value).is_integer():
        return str(int(value))
    return f"{value:.1f}"


def fallback_color(index):
    colors = ["#2ca02c", "#9467bd", "#ff7f0e", "#17becf"]
    return colors[index % len(colors)]


def escape(text):
    return (
        str(text)
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
    )


if __name__ == "__main__":
    main()
