from __future__ import annotations

from pathlib import Path
import os
import subprocess


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


def resolve_tpch_file(tpch_dir: Path, prefix: str, sf, suffix: str = ".tbl") -> Path:
    candidates = [tpch_dir / f"{prefix}_sf{token}{suffix}" for token in sf_tokens(sf)]
    for path in candidates:
        if path.exists():
            return path
    raise FileNotFoundError(
        f"Could not find {prefix} for sf={sf}. Tried: " + ", ".join(str(p.name) for p in candidates)
    )


def resolve_update_file(tpch_dir: Path, sf, pct, dist: str) -> Path:
    pct_token = f"{float(pct):g}" if isinstance(pct, (int, float)) else str(pct)
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
