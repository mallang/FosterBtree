from __future__ import annotations

from pathlib import Path
import os
import subprocess
import sys


TOL = {
    "blue": "#4477AA",
    "cyan": "#66CCEE",
    "green": "#228833",
    "yellow": "#CCBB44",
    "red": "#EE6677",
    "purple": "#AA3377",
    "grey": "#BBBBBB",
    "darkgreen": "#8C9916",
}

TABLE_DISPLAY = {
    "naive": "SNAP",
    "snap": "SNAP",
    "ivmh": "IVMH",
    "heap": "MONO",
    "chain": "DUAL",
    "par": "EPOCH",
}

REPAIR_DISPLAY = {
    "": "",
    "nr": "NR",
    "rr": "RR",
    "wr": "WR",
    "Nr": "NR",
    "Rr": "RR",
    "Wr": "WR",
    "No Repair": "NR",
    "Read Repair": "RR",
    "Write Repair": "WR",
}


def repo_root(start: str | Path | None = None) -> Path:
    cur = Path(start or os.getcwd()).resolve()
    if cur.is_file():
        cur = cur.parent
    for path in [cur, *cur.parents]:
        if (path / "Cargo.toml").exists():
            return path
    raise RuntimeError("Could not locate FosterBtree repo root")


def ensure_dirs(*paths: Path) -> None:
    for path in paths:
        path.mkdir(parents=True, exist_ok=True)


def apply_paper_style(root: Path) -> None:
    import matplotlib.pyplot as plt

    style_file = root / "benches" / "hash_join" / "custom_plt_sytle.mplstyle"
    if style_file.exists():
        plt.style.use(str(style_file))
    plt.rcParams["font.family"] = "serif"
    plt.rcParams["font.serif"] = [
        "Times New Roman",
        "Times",
        "Nimbus Roman No9 L",
        "DejaVu Serif",
    ]
    plt.rcParams["axes.spines.top"] = False
    plt.rcParams["axes.spines.right"] = False


def run_checked(cmd, cwd: Path, timeout: int | None = None, quiet: bool = False):
    if not quiet:
        print("$", " ".join(str(x) for x in cmd))
    result = subprocess.run(
        [str(x) for x in cmd],
        cwd=cwd,
        capture_output=True,
        text=True,
        timeout=timeout,
    )
    if result.returncode != 0:
        stderr = result.stderr[-4000:] if result.stderr else ""
        stdout = result.stdout[-2000:] if result.stdout else ""
        raise RuntimeError(
            f"Command failed with rc={result.returncode}\nSTDOUT:\n{stdout}\nSTDERR:\n{stderr}"
        )
    return result


def sf_tokens(sf) -> list[str]:
    text = str(sf).strip()
    out = [text]
    try:
        value = float(text)
        out.extend(
            [
                f"{value:g}",
                f"{value:.1f}",
                f"{value:.2f}",
            ]
        )
        if value.is_integer():
            out.append(str(int(value)))
    except ValueError:
        pass
    # Preserve order while removing duplicates.
    dedup = []
    for token in out:
        if token not in dedup:
            dedup.append(token)
    return dedup


def _canonical_sf_token(sf) -> str:
    return sf_tokens(sf)[0]


def _sigmod_dir_from_tpch(tpch_dir: Path) -> Path:
    tpch_dir = tpch_dir.resolve()
    if tpch_dir.name != "tpch_data":
        raise RuntimeError(f"Expected tpch_data directory, got {tpch_dir}")
    return tpch_dir.parent


def _ensure_dbgen(sigmod_dir: Path) -> Path:
    dbgen_dir = sigmod_dir / "dbgen"
    dbgen_bin = dbgen_dir / "dbgen"
    if dbgen_bin.exists():
        return dbgen_bin
    run_checked(["make"], dbgen_dir)
    if not dbgen_bin.exists():
        raise FileNotFoundError(f"dbgen build did not produce {dbgen_bin}")
    return dbgen_bin


def _generate_tpch_table(tpch_dir: Path, prefix: str, sf) -> Path:
    sigmod_dir = _sigmod_dir_from_tpch(tpch_dir)
    dbgen_bin = _ensure_dbgen(sigmod_dir)
    table_flag = {"part": "P", "lineitem": "L"}.get(prefix)
    if table_flag is None:
        raise FileNotFoundError(f"Automatic generation is not supported for prefix={prefix}")

    sf_token = _canonical_sf_token(sf)
    out_path = tpch_dir / f"{prefix}_sf{sf_token}.tbl"
    run_checked([str(dbgen_bin), "-s", str(sf), "-T", table_flag, "-f"], dbgen_bin.parent)
    raw_name = "part.tbl" if prefix == "part" else "lineitem.tbl"
    raw_path = dbgen_bin.parent / raw_name
    if not raw_path.exists():
        raise FileNotFoundError(f"dbgen did not produce {raw_path}")
    raw_path.replace(out_path)
    return out_path


def _generate_lineitem_probe(tpch_dir: Path, sf, suffix: str) -> Path:
    if suffix != "_1995-09-01_1995-10-01.tbl":
        raise FileNotFoundError(f"Automatic generation is only supported for Q14 probe suffix {suffix}")

    raw_lineitem = resolve_tpch_file(tpch_dir, "lineitem", sf, ".tbl")
    sf_token = _canonical_sf_token(sf)
    out_path = tpch_dir / f"lineitem_probe_sf{sf_token}{suffix}"

    with open(raw_lineitem) as fin, open(out_path, "w") as fout:
        for line in fin:
            fields = line.rstrip("\n").split("|")
            if len(fields) < 11:
                continue
            shipdate = fields[10]
            if "1995-09-01" <= shipdate < "1995-10-01":
                fout.write(f"{fields[1]}|{fields[5]}|{fields[6]}\n")
    return out_path


def resolve_tpch_file(tpch_dir: Path, prefix: str, sf, suffix: str = ".tbl") -> Path:
    candidates = [tpch_dir / f"{prefix}_sf{token}{suffix}" for token in sf_tokens(sf)]
    for path in candidates:
        if path.exists():
            return path
    if prefix in {"part", "lineitem"} and suffix == ".tbl":
        return _generate_tpch_table(tpch_dir, prefix, sf)
    if prefix == "lineitem_probe":
        return _generate_lineitem_probe(tpch_dir, sf, suffix)
    raise FileNotFoundError(
        f"Could not find {prefix} for sf={sf}. Tried: " + ", ".join(str(p.name) for p in candidates)
    )


def resolve_update_file(tpch_dir: Path, sf, pct, dist: str) -> Path:
    pct_token = f"{float(pct):g}" if isinstance(pct, (int, float)) else str(pct)
    for sf_token in sf_tokens(sf):
        path = tpch_dir / f"part_updates_sf{sf_token}_{pct_token}pct_{dist}.tbl"
        if path.exists():
            return path
    sigmod_dir = _sigmod_dir_from_tpch(tpch_dir)
    part_file = resolve_tpch_file(tpch_dir, "part", sf, ".tbl")
    cmd = [
        sys.executable,
        str(sigmod_dir / "generate_updates.py"),
        str(part_file),
        str(pct),
        str(sf),
        "--output-dir",
        str(tpch_dir),
    ]
    if dist != "uniform":
        if not dist.startswith("zipf"):
            raise FileNotFoundError(
                f"Could not find update file for sf={sf}, pct={pct}, dist={dist}"
            )
        theta = dist.removeprefix("zipf")
        cmd.extend(["--zipf", theta])
    run_checked(cmd, sigmod_dir)
    for sf_token in sf_tokens(sf):
        path = tpch_dir / f"part_updates_sf{sf_token}_{pct_token}pct_{dist}.tbl"
        if path.exists():
            return path
    raise FileNotFoundError(
        f"Could not find update file for sf={sf}, pct={pct}, dist={dist}"
    )


def display_name(table_type: str, repair_mode: str = "") -> str:
    table = TABLE_DISPLAY.get(table_type, table_type.upper())
    repair = REPAIR_DISPLAY.get(repair_mode, repair_mode.upper())
    return table if not repair else f"{table}-{repair}"


def normalize_table(table_type: str) -> str:
    text = str(table_type)
    mapping = {
        "Naive": "naive",
        "Snap": "snap",
        "Ivmh": "ivmh",
        "Heap": "heap",
        "Chain": "chain",
        "Par": "par",
    }
    return mapping.get(text, text.lower())


def normalize_repair(repair_mode: str) -> str:
    return REPAIR_DISPLAY.get(str(repair_mode), str(repair_mode).upper())
