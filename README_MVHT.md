# MVHT — Multi-Version Hash Tables (paper artifact)

This repository hosts the prototype for the paper *Multi-Version Hash Tables:
Reusing Derived Intermediate Operator State*. The MVHT prototype is built
inside a larger research storage engine, so the top-level `README.md`
describes that engine rather than MVHT. **This file is the entry point for
the artifact.**

## Where the code is

All MVHT structures live under `src/mvcc_index/`; the single-version
baselines live under `src/naive_hash_index/`.

| Paper | Module | Main type |
|---|---|---|
| `MONO` (single chain per bucket) | `src/mvcc_index/hash_heap/` | `HeapHashTable` |
| `DUAL` (current + historical chains) | `src/mvcc_index/dual_heap_hash/` | `ChainedHashTable` |
| `EPOCH` (epoch-partitioned chains) | `src/mvcc_index/ts_partitioned/` | `TsPartitionedTable` |
| `SNAP` (snapshot rebuild / cache baseline) | `src/naive_hash_index/naive_mvht.rs` | `NaiveMvHashTable` |
| `IVMH` (current-state IVM baseline) | `src/naive_hash_index/ivmh.rs` | `IvmHashTable` |

Shared infrastructure:

| Component | Path |
|---|---|
| `MvccIndex` trait, versioned record layout, visibility rules | `src/mvcc_index/mod.rs` |
| Bucket / page layout and per-page timestamp summaries | `src/mvcc_index/hash_join_page.rs`, `src/mvcc_index/hash_common.rs` |
| Uniform benchmark interface over all five designs | `benches/hash_join/htap_simulation/interface.rs` |

Repair timing (`NR` / `RR` / `WR`) is selected per run via the `RepairMode`
enum in each benchmark driver, not by a separate module.

## Experiments

Each experiment directory contains the driver, the raw measurements under
`data/`, and the notebook that produces the figure in the paper.

| Paper | Directory |
|---|---|
| Exp. 1 — design space (Fig. 3) | `benches/sigmod_exp1_design_space/` |
| Exp. 1 — size footprint (Fig. 4) | `benches/sigmod_exp1_space_breakdown/` |
| Exp. 2 — historical / delta crossover (Fig. 5) | `benches/sigmod_exp2_scanonly_crossover/`, `benches/sigmod_exp2_scanmix_crossover/` |
| Exp. 3 — TPC-H Q14 join-update-join (Fig. 6) | `benches/sigmod_exp3_join_update_join/` |
| Exp. 4 — concurrent HTAP trace (Fig. 7) | `benches/sigmod_exp4_concurrency/` |
| Exp. 5 — symmetric hash join (Fig. 8) | `benches/sigmod_exp5_symmetric_join/` |

## Building

```bash
cargo build --release            # rustc 1.82.0, x86_64-unknown-linux-gnu
```

See `install.sh` for the toolchain and profiling dependencies.
