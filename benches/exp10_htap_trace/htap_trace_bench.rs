use clap::{Parser, ValueEnum};
use fbtree::bp::{get_in_mem_pool, ContainerKey, InMemPool, MemPool};
use fbtree::mvcc_index::hash_common::{KVWithTs, RowDelta};
use fbtree::mvcc_index::dual_heap_hash::chained_hash_table::ChainedHashTable;
use fbtree::mvcc_index::hash_heap::hash_heap_table::HeapHashTable;
use fbtree::mvcc_index::hash_join_page::record::RecordRef;
use fbtree::mvcc_index::ts_partitioned::ts_partitioned_table::TsPartitionedTable;
use fbtree::mvcc_index::{Delta, MvccIndex, VersionsMap};
use fbtree::naive_hash_index::{HeapBaseMvccTable, HeapHashChain, NaiveHashTable};
use fbtree::prelude::{AccessMethodError, Timestamp};
use parking_lot::{Condvar, Mutex, RwLock};
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::error::Error;
use std::fs::{metadata, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Instant;

const INITIAL_TS: Timestamp = 1;

#[derive(Debug, Clone, Copy, ValueEnum)]
enum TableType {
    Chain,
    Heap,
    Par,
    Snap,
    Ivmh,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum RepairMode {
    Nr,
    Rr,
    Wr,
}

#[derive(Parser, Debug, Clone)]
#[command(about = "Mixed HTAP trace benchmark over join/scan/update transactions")]
struct Cli {
    #[arg(long)]
    part_file: String,

    #[arg(long)]
    lineitem_file: String,

    #[arg(long)]
    updates_file: String,

    #[arg(long, value_enum, default_value = "heap")]
    table_type: TableType,

    #[arg(long, value_enum, default_value = "wr")]
    repair_mode: RepairMode,

    #[arg(long, default_value_t = 4096)]
    bucket_num: usize,

    #[arg(long, default_value_t = 5)]
    worker_threads: usize,

    #[arg(long, default_value_t = 40)]
    join_txs: usize,

    #[arg(long, default_value_t = 20)]
    scan_txs: usize,

    #[arg(long, default_value_t = 10)]
    delta_txs: usize,

    #[arg(long, default_value_t = 30)]
    update_waves: usize,

    #[arg(long, default_value_t = 2)]
    readable_every: usize,

    #[arg(long, default_value_t = 0.3)]
    historical_ratio: f64,

    #[arg(long, default_value_t = 1)]
    warmup: usize,

    #[arg(long, default_value_t = 5)]
    repeat: usize,

    #[arg(long, default_value_t = 1)]
    trim: usize,

    #[arg(long, default_value_t = 0.0)]
    update_pct: f64,

    #[arg(long)]
    output_csv: Option<String>,
}

#[derive(Clone)]
struct PartEntry {
    partkey: Vec<u8>,
    ptype: Vec<u8>,
}

#[derive(Clone)]
struct ProbeRow {
    partkey: Vec<u8>,
}

#[derive(Clone)]
struct UpdateOp {
    partkey: Vec<u8>,
    new_ptype: Vec<u8>,
}

#[derive(Clone, Copy)]
enum ReadShape {
    Join,
    Scan,
}

#[derive(Clone)]
enum TraceTx {
    Read {
        shape: ReadShape,
        target_ts: Timestamp,
    },
    Delta {
        from_ts: Timestamp,
        to_ts: Timestamp,
    },
    Update {
        wave_idx: usize,
        commit_ts: Timestamp,
    },
}

#[derive(Clone, Copy, Default)]
struct TxMetrics {
    latency_ms: f64,
}

#[derive(Clone, Default)]
struct WorkerStats {
    join_latencies: Vec<f64>,
    scan_latencies: Vec<f64>,
    delta_latencies: Vec<f64>,
    update_latencies: Vec<f64>,
}

impl WorkerStats {
    fn merge(&mut self, other: WorkerStats) {
        self.join_latencies.extend(other.join_latencies);
        self.scan_latencies.extend(other.scan_latencies);
        self.delta_latencies.extend(other.delta_latencies);
        self.update_latencies.extend(other.update_latencies);
    }
}

#[derive(Clone, Default)]
struct TraceResult {
    total_ms: f64,
    avg_join_latency_ms: f64,
    p95_join_latency_ms: f64,
    avg_scan_latency_ms: f64,
    p95_scan_latency_ms: f64,
    avg_delta_latency_ms: f64,
    avg_update_latency_ms: f64,
}

type SnapshotMap = BTreeMap<Timestamp, Arc<NaiveHashTable<InMemPool>>>;
type ArcMvccIndex = Arc<
    dyn MvccIndex<
            InMemPool,
            Key = Vec<u8>,
            PKey = Vec<u8>,
            Value = Vec<u8>,
            Error = AccessMethodError,
        > + Send
        + Sync,
>;

#[derive(Default)]
struct SnapshotBuildState {
    building: HashSet<Timestamp>,
}

struct CommitTracker {
    state: Mutex<Timestamp>,
    cv: Condvar,
}

impl CommitTracker {
    fn new() -> Self {
        Self {
            state: Mutex::new(INITIAL_TS),
            cv: Condvar::new(),
        }
    }

    fn committed(&self) -> Timestamp {
        *self.state.lock()
    }

    fn wait_until_visible(&self, target_ts: Timestamp) {
        let mut committed = self.state.lock();
        while *committed < target_ts {
            self.cv.wait(&mut committed);
        }
    }

    fn wait_for_turn(&self, commit_ts: Timestamp) {
        let mut committed = self.state.lock();
        while *committed + 1 != commit_ts {
            self.cv.wait(&mut committed);
        }
    }

    fn publish(&self, commit_ts: Timestamp) {
        *self.state.lock() = commit_ts;
        self.cv.notify_all();
    }
}

trait HtapTraceTable: Send + Sync {
    fn run_join_tx(&self, target_ts: Timestamp, probes: &[ProbeRow]) -> TxMetrics;
    fn run_scan_tx(&self, target_ts: Timestamp) -> TxMetrics;
    fn run_delta_tx(&self, from_ts: Timestamp, to_ts: Timestamp) -> TxMetrics;
    fn run_update_tx(&self, wave_idx: usize, commit_ts: Timestamp) -> TxMetrics;
}

fn parse_tpch_line(line: &str) -> Option<Vec<&str>> {
    let mut fields: Vec<&str> = line.split('|').collect();
    if fields.last().is_some_and(|f| f.is_empty()) {
        fields.pop();
    }
    if fields.is_empty() {
        None
    } else {
        Some(fields)
    }
}

fn normalize_ptype_value(raw: &[u8]) -> Vec<u8> {
    const PTYPE_VALUE_SIZE: usize = 32;
    let mut out = vec![b' '; PTYPE_VALUE_SIZE];
    let n = raw.len().min(PTYPE_VALUE_SIZE);
    out[..n].copy_from_slice(&raw[..n]);
    out
}

fn make_wave_ptype(raw: &[u8], wave_idx: usize) -> Vec<u8> {
    let mut out = normalize_ptype_value(raw);
    let tag = format!("W{:05}", wave_idx + 1);
    let start = out.len().saturating_sub(tag.len());
    out[start..start + tag.len()].copy_from_slice(tag.as_bytes());
    out
}

fn read_part_table(path: &str) -> Result<Vec<PartEntry>, Box<dyn Error>> {
    let file = std::fs::File::open(path)?;
    let reader = BufReader::new(file);
    let mut entries = Vec::new();
    for line in reader.lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        let Some(fields) = parse_tpch_line(&line) else {
            continue;
        };
        if fields.len() < 5 {
            continue;
        }
        entries.push(PartEntry {
            partkey: fields[0].as_bytes().to_vec(),
            ptype: normalize_ptype_value(fields[4].as_bytes()),
        });
    }
    Ok(entries)
}

fn read_probe_rows(path: &str) -> Result<Vec<ProbeRow>, Box<dyn Error>> {
    let file = std::fs::File::open(path)?;
    let reader = BufReader::new(file);
    let mut rows = Vec::new();
    for line in reader.lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        let Some(fields) = parse_tpch_line(&line) else {
            continue;
        };
        if fields.is_empty() {
            continue;
        }
        rows.push(ProbeRow {
            partkey: fields[0].as_bytes().to_vec(),
        });
    }
    Ok(rows)
}

fn read_updates(path: &str) -> Result<Vec<UpdateOp>, Box<dyn Error>> {
    let file = std::fs::File::open(path)?;
    let reader = BufReader::new(file);
    let mut ops = Vec::new();
    for line in reader.lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        let Some(fields) = parse_tpch_line(&line) else {
            continue;
        };
        if fields.len() < 5 {
            continue;
        }
        ops.push(UpdateOp {
            partkey: fields[0].as_bytes().to_vec(),
            new_ptype: normalize_ptype_value(fields[4].as_bytes()),
        });
    }
    Ok(ops)
}

fn build_update_waves(base_updates: &[UpdateOp], update_waves: usize) -> Vec<Vec<UpdateOp>> {
    (0..update_waves)
        .map(|wave_idx| {
            base_updates
                .iter()
                .map(|op| UpdateOp {
                    partkey: op.partkey.clone(),
                    new_ptype: make_wave_ptype(&op.new_ptype, wave_idx),
                })
                .collect()
        })
        .collect()
}

fn build_snapshot_from_rows<I, K, P, V>(
    c_key: ContainerKey,
    mem_pool: Arc<InMemPool>,
    bucket_num: usize,
    rows: I,
) -> Arc<NaiveHashTable<InMemPool>>
where
    I: IntoIterator<Item = (K, P, V)>,
    K: AsRef<[u8]>,
    P: AsRef<[u8]>,
    V: AsRef<[u8]>,
{
    let table = Arc::new(NaiveHashTable::new_with_bucket_num(c_key, mem_pool, bucket_num));
    for (key, pkey, value) in rows {
        table
            .insert(RecordRef::new(key.as_ref(), pkey.as_ref(), value.as_ref()))
            .unwrap();
    }
    table
}

fn materialize_snapshot(
    snapshots: &RwLock<SnapshotMap>,
    build_state: &Mutex<SnapshotBuildState>,
    build_cv: &Condvar,
    ts: Timestamp,
    builder: impl FnOnce() -> Arc<NaiveHashTable<InMemPool>>,
) -> Arc<NaiveHashTable<InMemPool>> {
    if let Some(snapshot) = snapshots.read().get(&ts).cloned() {
        return snapshot;
    }

    let mut builder = Some(builder);
    loop {
        if let Some(snapshot) = snapshots.read().get(&ts).cloned() {
            return snapshot;
        }

        let mut state = build_state.lock();
        if let Some(snapshot) = snapshots.read().get(&ts).cloned() {
            return snapshot;
        }
        if !state.building.contains(&ts) {
            state.building.insert(ts);
            drop(state);

            let snapshot = builder.take().unwrap()();
            snapshots.write().insert(ts, snapshot.clone());

            let mut state = build_state.lock();
            state.building.remove(&ts);
            build_cv.notify_all();
            return snapshot;
        }
        build_cv.wait(&mut state);
    }
}

fn spread_marks(total: usize, marked: usize) -> Vec<bool> {
    let mut marks = vec![false; total];
    if total == 0 || marked == 0 {
        return marks;
    }
    let mut filled = 0usize;
    for i in 0..marked {
        let mut pos = (((i as f64) + 0.5) * total as f64 / marked as f64).floor() as usize;
        pos = pos.min(total - 1);
        while pos < total && marks[pos] {
            pos += 1;
        }
        if pos == total {
            pos = total - 1;
            while marks[pos] && pos > 0 {
                pos -= 1;
            }
        }
        if !marks[pos] {
            marks[pos] = true;
            filled += 1;
        }
    }
    if filled < marked {
        for mark in &mut marks {
            if !*mark {
                *mark = true;
                filled += 1;
                if filled == marked {
                    break;
                }
            }
        }
    }
    marks
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum PendingReadShape {
    Join,
    Scan,
    Delta,
}

fn build_read_queue(join_txs: usize, scan_txs: usize, delta_txs: usize) -> VecDeque<PendingReadShape> {
    let total_reads = join_txs + scan_txs + delta_txs;
    let delta_marks = spread_marks(total_reads, delta_txs.min(total_reads));
    let non_delta_total = total_reads.saturating_sub(delta_txs.min(total_reads));
    let scan_marks = spread_marks(non_delta_total, scan_txs.min(non_delta_total));

    let mut queue = VecDeque::with_capacity(total_reads);
    let mut non_delta_idx = 0usize;
    for pos in 0..total_reads {
        if delta_marks[pos] {
            queue.push_back(PendingReadShape::Delta);
        } else {
            let shape = if scan_marks.get(non_delta_idx).copied().unwrap_or(false) {
                PendingReadShape::Scan
            } else {
                PendingReadShape::Join
            };
            non_delta_idx += 1;
            queue.push_back(shape);
        }
    }
    queue
}

fn pop_next_read_shape(
    queue: &mut VecDeque<PendingReadShape>,
    delta_available: bool,
) -> Option<PendingReadShape> {
    if queue.is_empty() {
        return None;
    }

    let attempts = queue.len();
    for _ in 0..attempts {
        let shape = queue.pop_front().unwrap();
        if shape != PendingReadShape::Delta || delta_available {
            return Some(shape);
        }
        queue.push_back(shape);
    }

    queue.pop_front()
}

fn readable_epochs(update_waves: usize, readable_every: usize) -> Vec<Timestamp> {
    let readable_every = readable_every.max(1);
    let mut epochs = vec![INITIAL_TS];
    for wave_idx in 0..update_waves {
        let wave_no = wave_idx + 1;
        let commit_ts = INITIAL_TS + wave_no as u64;
        if wave_no % readable_every == 0 || wave_no == update_waves {
            epochs.push(commit_ts);
        }
    }
    epochs
}

fn historical_target(current_ts: Timestamp, readable_ts: &[Timestamp], hist_idx: usize) -> Timestamp {
    let eligible: Vec<_> = readable_ts
        .iter()
        .copied()
        .filter(|ts| *ts < current_ts)
        .collect();
    if eligible.is_empty() {
        INITIAL_TS
    } else {
        eligible[hist_idx % eligible.len()]
    }
}

fn delta_target(
    current_ts: Timestamp,
    readable_ts: &[Timestamp],
    delta_idx: usize,
) -> Option<(Timestamp, Timestamp)> {
    let all_pairs: Vec<_> = readable_ts.windows(2).map(|w| (w[0], w[1])).collect();
    if all_pairs.is_empty() {
        return None;
    }

    let available: Vec<_> = all_pairs
        .iter()
        .copied()
        .filter(|(_, to_ts)| *to_ts <= current_ts)
        .collect();
    if available.is_empty() {
        Some(all_pairs[0])
    } else {
        Some(available[delta_idx % available.len()])
    }
}

fn build_trace(
    join_txs: usize,
    scan_txs: usize,
    delta_txs: usize,
    update_waves: usize,
    readable_every: usize,
    historical_ratio: f64,
) -> (Vec<TraceTx>, Vec<Timestamp>) {
    let total_reads = join_txs + scan_txs + delta_txs;
    let non_delta_reads = join_txs + scan_txs;
    let readable_ts = readable_epochs(update_waves, readable_every);
    let mut read_queue = build_read_queue(join_txs, scan_txs, delta_txs);
    let historical_count = ((non_delta_reads as f64) * historical_ratio).round() as usize;
    let mut historical_marks =
        VecDeque::from(spread_marks(non_delta_reads, historical_count.min(non_delta_reads)));
    let reads_per_wave = if update_waves == 0 {
        total_reads.max(1)
    } else {
        (total_reads + update_waves - 1) / update_waves
    };

    let mut trace = Vec::with_capacity(total_reads + update_waves);
    let mut current_ts = INITIAL_TS;
    let mut emitted_updates = 0usize;
    let mut hist_idx = 0usize;
    let mut delta_idx = 0usize;

    for read_slot in 0..total_reads {
        let delta_available = readable_ts
            .windows(2)
            .any(|pair| pair[1] <= current_ts);
        let shape = pop_next_read_shape(&mut read_queue, delta_available)
            .expect("read queue should not underflow");
        match shape {
            PendingReadShape::Join | PendingReadShape::Scan => {
                let is_historical = historical_marks.pop_front().unwrap_or(false);
                let target_ts = if is_historical {
                    let ts = historical_target(current_ts, &readable_ts, hist_idx);
                    hist_idx += 1;
                    ts
                } else {
                    current_ts
                };
                let shape = match shape {
                    PendingReadShape::Join => ReadShape::Join,
                    PendingReadShape::Scan => ReadShape::Scan,
                    PendingReadShape::Delta => unreachable!(),
                };
                trace.push(TraceTx::Read { shape, target_ts });
            }
            PendingReadShape::Delta => {
                if let Some((from_ts, to_ts)) = delta_target(current_ts, &readable_ts, delta_idx) {
                    delta_idx += 1;
                    trace.push(TraceTx::Delta { from_ts, to_ts });
                } else {
                    trace.push(TraceTx::Read {
                        shape: ReadShape::Join,
                        target_ts: current_ts,
                    });
                }
            }
        }

        while emitted_updates < update_waves
            && (read_slot + 1) >= ((emitted_updates + 1) * reads_per_wave).min(total_reads)
        {
            emitted_updates += 1;
            current_ts = INITIAL_TS + emitted_updates as u64;
            trace.push(TraceTx::Update {
                wave_idx: emitted_updates - 1,
                commit_ts: current_ts,
            });
        }
    }

    while emitted_updates < update_waves {
        emitted_updates += 1;
        current_ts = INITIAL_TS + emitted_updates as u64;
        trace.push(TraceTx::Update {
            wave_idx: emitted_updates - 1,
            commit_ts: current_ts,
        });
    }

    (trace, readable_ts)
}

fn probe_naive_table<T: MemPool + 'static>(table: &NaiveHashTable<T>, probes: &[ProbeRow]) {
    let mut results = Vec::new();
    for row in probes {
        results.clear();
        table.scan_key_vec(&row.partkey, &mut results).unwrap();
    }
}

fn scan_naive_table<T: MemPool + 'static>(table: &NaiveHashTable<T>) {
    let _count = table.scan().unwrap().count();
}

fn scan_naive_delta_tables(
    from: &NaiveHashTable<InMemPool>,
    to: &NaiveHashTable<InMemPool>,
    bucket_num: usize,
) {
    let mut delta_map = HashMap::<Vec<u8>, RowDelta>::new();
    for bucket_idx in 0..bucket_num {
        let from_bucket = from.get_chain(bucket_idx);
        let to_bucket = to.get_chain(bucket_idx);
        HeapHashChain::scan_deltas(&from_bucket, &to_bucket, &mut delta_map).unwrap();
    }

    let _count = delta_map
        .into_iter()
        .filter_map(|(pk, from_to_delta)| {
            let (from_kv, to_kv) = from_to_delta.split();
            if to_kv == KVWithTs::default() {
                None
            } else if from_kv == KVWithTs::default() {
                Some((to_kv.get_k().to_vec(), pk, Delta::Inserted(to_kv.get_v().to_vec())))
            } else if from_kv.get_v() == to_kv.get_v() {
                None
            } else {
                Some((to_kv.get_k().to_vec(), pk, Delta::Updated(to_kv.get_v().to_vec())))
            }
        })
        .count();
}

struct SnapTrace {
    c_key: ContainerKey,
    mem_pool: Arc<InMemPool>,
    bucket_num: usize,
    base_table: HeapBaseMvccTable,
    snapshots: RwLock<SnapshotMap>,
    build_state: Mutex<SnapshotBuildState>,
    build_cv: Condvar,
    commit_tracker: CommitTracker,
}

impl SnapTrace {
    fn new(parts: &[PartEntry], update_waves: &[Vec<UpdateOp>], bucket_num: usize) -> Self {
        let mem_pool = get_in_mem_pool();
        let c_key = ContainerKey::new(0, 0);
        let base_table = HeapBaseMvccTable::new();
        for part in parts {
            base_table.insert_at_ts(&part.partkey, &part.partkey, &part.ptype, INITIAL_TS);
        }
        for (wave_idx, updates) in update_waves.iter().enumerate() {
            let ts = INITIAL_TS + 1 + wave_idx as u64;
            for update in updates {
                base_table.update_at_ts(&update.partkey, &update.partkey, &update.new_ptype, ts);
            }
        }
        Self {
            c_key,
            mem_pool,
            bucket_num,
            base_table,
            snapshots: RwLock::new(BTreeMap::new()),
            build_state: Mutex::new(SnapshotBuildState::default()),
            build_cv: Condvar::new(),
            commit_tracker: CommitTracker::new(),
        }
    }

    fn build_snapshot_from_base(&self, ts: Timestamp) -> Arc<NaiveHashTable<InMemPool>> {
        build_snapshot_from_rows(
            self.c_key,
            self.mem_pool.clone(),
            self.bucket_num,
            self.base_table.scan_as_of(ts).into_iter(),
        )
    }

    fn get_or_build_snapshot(&self, ts: Timestamp) -> Arc<NaiveHashTable<InMemPool>> {
        materialize_snapshot(
            &self.snapshots,
            &self.build_state,
            &self.build_cv,
            ts,
            || self.build_snapshot_from_base(ts),
        )
    }
}

impl HtapTraceTable for SnapTrace {
    fn run_join_tx(&self, target_ts: Timestamp, probes: &[ProbeRow]) -> TxMetrics {
        let start = Instant::now();
        self.commit_tracker.wait_until_visible(target_ts);
        let snapshot = self.get_or_build_snapshot(target_ts);
        probe_naive_table(snapshot.as_ref(), probes);
        TxMetrics {
            latency_ms: start.elapsed().as_secs_f64() * 1000.0,
        }
    }

    fn run_scan_tx(&self, target_ts: Timestamp) -> TxMetrics {
        let start = Instant::now();
        self.commit_tracker.wait_until_visible(target_ts);
        let snapshot = self.get_or_build_snapshot(target_ts);
        scan_naive_table(snapshot.as_ref());
        TxMetrics {
            latency_ms: start.elapsed().as_secs_f64() * 1000.0,
        }
    }

    fn run_delta_tx(&self, from_ts: Timestamp, to_ts: Timestamp) -> TxMetrics {
        let start = Instant::now();
        self.commit_tracker.wait_until_visible(to_ts);
        let from = self.get_or_build_snapshot(from_ts);
        let to = self.get_or_build_snapshot(to_ts);
        scan_naive_delta_tables(from.as_ref(), to.as_ref(), self.bucket_num);
        TxMetrics {
            latency_ms: start.elapsed().as_secs_f64() * 1000.0,
        }
    }

    fn run_update_tx(&self, _wave_idx: usize, commit_ts: Timestamp) -> TxMetrics {
        let start = Instant::now();
        self.commit_tracker.wait_for_turn(commit_ts);
        self.commit_tracker.publish(commit_ts);
        TxMetrics {
            latency_ms: start.elapsed().as_secs_f64() * 1000.0,
        }
    }
}

struct IvmhTrace {
    c_key: ContainerKey,
    mem_pool: Arc<InMemPool>,
    bucket_num: usize,
    base_table: HeapBaseMvccTable,
    current_table: RwLock<NaiveHashTable<InMemPool>>,
    update_waves: Vec<Vec<UpdateOp>>,
    snapshots: RwLock<SnapshotMap>,
    build_state: Mutex<SnapshotBuildState>,
    build_cv: Condvar,
    commit_tracker: CommitTracker,
}

impl IvmhTrace {
    fn new(parts: &[PartEntry], update_waves: &[Vec<UpdateOp>], bucket_num: usize) -> Self {
        let mem_pool = get_in_mem_pool();
        let c_key = ContainerKey::new(0, 0);
        let base_table = HeapBaseMvccTable::new();
        let current_table = NaiveHashTable::new_with_bucket_num(c_key, mem_pool.clone(), bucket_num);
        for part in parts {
            base_table.insert_at_ts(&part.partkey, &part.partkey, &part.ptype, INITIAL_TS);
            current_table
                .insert(RecordRef::new(&part.partkey, &part.partkey, &part.ptype))
                .unwrap();
        }
        for (wave_idx, updates) in update_waves.iter().enumerate() {
            let ts = INITIAL_TS + 1 + wave_idx as u64;
            for update in updates {
                base_table.update_at_ts(&update.partkey, &update.partkey, &update.new_ptype, ts);
            }
        }
        Self {
            c_key,
            mem_pool,
            bucket_num,
            base_table,
            current_table: RwLock::new(current_table),
            update_waves: update_waves.to_vec(),
            snapshots: RwLock::new(BTreeMap::new()),
            build_state: Mutex::new(SnapshotBuildState::default()),
            build_cv: Condvar::new(),
            commit_tracker: CommitTracker::new(),
        }
    }

    fn build_snapshot_from_base(&self, ts: Timestamp) -> Arc<NaiveHashTable<InMemPool>> {
        build_snapshot_from_rows(
            self.c_key,
            self.mem_pool.clone(),
            self.bucket_num,
            self.base_table.scan_as_of(ts).into_iter(),
        )
    }

    fn get_or_build_snapshot(&self, ts: Timestamp) -> Arc<NaiveHashTable<InMemPool>> {
        materialize_snapshot(
            &self.snapshots,
            &self.build_state,
            &self.build_cv,
            ts,
            || self.build_snapshot_from_base(ts),
        )
    }

    fn with_exact_table<R>(&self, target_ts: Timestamp, f: impl FnOnce(&NaiveHashTable<InMemPool>) -> R) -> R {
        loop {
            self.commit_tracker.wait_until_visible(target_ts);
            let committed = self.commit_tracker.committed();
            if committed == target_ts {
                let guard = self.current_table.read();
                if self.commit_tracker.committed() == target_ts {
                    return f(&guard);
                }
                drop(guard);
                thread::yield_now();
                continue;
            }
            let snapshot = self.get_or_build_snapshot(target_ts);
            return f(snapshot.as_ref());
        }
    }
}

impl HtapTraceTable for IvmhTrace {
    fn run_join_tx(&self, target_ts: Timestamp, probes: &[ProbeRow]) -> TxMetrics {
        let start = Instant::now();
        self.with_exact_table(target_ts, |table| probe_naive_table(table, probes));
        TxMetrics {
            latency_ms: start.elapsed().as_secs_f64() * 1000.0,
        }
    }

    fn run_scan_tx(&self, target_ts: Timestamp) -> TxMetrics {
        let start = Instant::now();
        self.with_exact_table(target_ts, scan_naive_table);
        TxMetrics {
            latency_ms: start.elapsed().as_secs_f64() * 1000.0,
        }
    }

    fn run_delta_tx(&self, from_ts: Timestamp, to_ts: Timestamp) -> TxMetrics {
        let start = Instant::now();
        self.commit_tracker.wait_until_visible(to_ts);
        let from = self.get_or_build_snapshot(from_ts);
        let to = self.get_or_build_snapshot(to_ts);
        scan_naive_delta_tables(from.as_ref(), to.as_ref(), self.bucket_num);
        TxMetrics {
            latency_ms: start.elapsed().as_secs_f64() * 1000.0,
        }
    }

    fn run_update_tx(&self, wave_idx: usize, commit_ts: Timestamp) -> TxMetrics {
        let start = Instant::now();
        self.commit_tracker.wait_for_turn(commit_ts);
        {
            let current = self.current_table.write();
            for update in &self.update_waves[wave_idx] {
                current
                    .update(RecordRef::new(&update.partkey, &update.partkey, &update.new_ptype))
                    .unwrap();
            }
        }
        self.commit_tracker.publish(commit_ts);
        TxMetrics {
            latency_ms: start.elapsed().as_secs_f64() * 1000.0,
        }
    }
}

struct MvhtTrace {
    table: ArcMvccIndex,
    table_type: TableType,
    repair_mode: RepairMode,
    update_waves: Vec<Vec<UpdateOp>>,
    readable_commits: HashSet<Timestamp>,
    commit_tracker: CommitTracker,
    partition_guard: RwLock<()>,
}

impl MvhtTrace {
    fn new(
        parts: &[PartEntry],
        table_type: TableType,
        repair_mode: RepairMode,
        bucket_num: usize,
        update_waves: &[Vec<UpdateOp>],
        readable_ts: &[Timestamp],
    ) -> Self {
        let mem_pool = get_in_mem_pool();
        let c_key = ContainerKey::new(0, 0);
        let table: ArcMvccIndex = match table_type {
            TableType::Chain => Arc::new(
                ChainedHashTable::create_with_bucket_num(c_key, mem_pool.clone(), bucket_num).unwrap(),
            ),
            TableType::Heap => Arc::new(
                HeapHashTable::create_with_bucket_num(c_key, mem_pool.clone(), bucket_num).unwrap(),
            ),
            TableType::Par => Arc::new(
                TsPartitionedTable::create_with_bucket_num(c_key, mem_pool.clone(), bucket_num).unwrap(),
            ),
            _ => panic!("invalid MVHT type"),
        };
        for (i, part) in parts.iter().enumerate() {
            table
                .insert(
                    part.partkey.clone(),
                    part.partkey.clone(),
                    INITIAL_TS,
                    i as u64,
                    part.ptype.clone(),
                )
                .unwrap();
        }
        if matches!(table_type, TableType::Par) {
            table.split_at_ts(INITIAL_TS + 1).unwrap();
        }
        Self {
            table,
            table_type,
            repair_mode,
            update_waves: update_waves.to_vec(),
            readable_commits: readable_ts.iter().copied().collect(),
            commit_tracker: CommitTracker::new(),
            partition_guard: RwLock::new(()),
        }
    }
}

impl HtapTraceTable for MvhtTrace {
    fn run_join_tx(&self, target_ts: Timestamp, probes: &[ProbeRow]) -> TxMetrics {
        let start = Instant::now();
        self.commit_tracker.wait_until_visible(target_ts);
        let _partition_read_guard = if matches!(self.table_type, TableType::Par) {
            Some(self.partition_guard.read())
        } else {
            None
        };
        let mut nr_buf = HashMap::new();
        let mut rr_dedup = HashMap::new();
        let mut rr_versions: VersionsMap = HashMap::new();
        for row in probes {
            loop {
                let result = match self.repair_mode {
                    RepairMode::Nr => self.table.scan_key_vec_nr(&row.partkey, target_ts, &mut nr_buf),
                    RepairMode::Rr => self.table.scan_key_vec_rr(
                        &row.partkey,
                        target_ts,
                        &mut rr_dedup,
                        &mut rr_versions,
                    ),
                    RepairMode::Wr => self.table.scan_key_vec(&row.partkey, target_ts),
                };
                match result {
                    Ok(_) => break,
                    Err(AccessMethodError::PageReadLatchFailed | AccessMethodError::PageWriteLatchFailed) => {
                        thread::yield_now();
                    }
                    Err(err) => panic!("MVHT join read failed: {}", err),
                }
            }
        }
        TxMetrics {
            latency_ms: start.elapsed().as_secs_f64() * 1000.0,
        }
    }

    fn run_scan_tx(&self, target_ts: Timestamp) -> TxMetrics {
        let start = Instant::now();
        self.commit_tracker.wait_until_visible(target_ts);
        let _partition_read_guard = if matches!(self.table_type, TableType::Par) {
            Some(self.partition_guard.read())
        } else {
            None
        };
        loop {
            match self.table.scan(target_ts) {
                Ok(iter) => {
                    let _count = iter.count();
                    break;
                }
                Err(AccessMethodError::PageReadLatchFailed | AccessMethodError::PageWriteLatchFailed) => {
                    thread::yield_now();
                }
                Err(err) => panic!("MVHT scan failed: {}", err),
            }
        }
        TxMetrics {
            latency_ms: start.elapsed().as_secs_f64() * 1000.0,
        }
    }

    fn run_delta_tx(&self, from_ts: Timestamp, to_ts: Timestamp) -> TxMetrics {
        let start = Instant::now();
        self.commit_tracker.wait_until_visible(to_ts);
        let _partition_read_guard = if matches!(self.table_type, TableType::Par) {
            Some(self.partition_guard.read())
        } else {
            None
        };
        loop {
            let result = match self.repair_mode {
                RepairMode::Rr => self.table.delta_scan_read_repair(from_ts, to_ts),
                _ => self.table.delta_scan(from_ts, to_ts),
            };
            match result {
                Ok(iter) => {
                    let _count = iter.count();
                    break;
                }
                Err(AccessMethodError::PageReadLatchFailed | AccessMethodError::PageWriteLatchFailed) => {
                    thread::yield_now();
                }
                Err(err) => panic!("MVHT delta scan failed: {}", err),
            }
        }
        TxMetrics {
            latency_ms: start.elapsed().as_secs_f64() * 1000.0,
        }
    }

    fn run_update_tx(&self, wave_idx: usize, commit_ts: Timestamp) -> TxMetrics {
        let start = Instant::now();
        self.commit_tracker.wait_for_turn(commit_ts);
        for update in &self.update_waves[wave_idx] {
            loop {
                let result = match self.repair_mode {
                    RepairMode::Wr => self.table.update_write_repair(
                        update.partkey.clone(),
                        update.partkey.clone(),
                        commit_ts,
                        0,
                        update.new_ptype.clone(),
                    ),
                    _ => self.table.update(
                        update.partkey.clone(),
                        update.partkey.clone(),
                        commit_ts,
                        0,
                        update.new_ptype.clone(),
                    ),
                };
                match result {
                    Ok(_) => break,
                    Err(AccessMethodError::PageReadLatchFailed | AccessMethodError::PageWriteLatchFailed) => {
                        thread::yield_now();
                    }
                    Err(err) => panic!("MVHT update failed: {}", err),
                }
            }
        }
        if matches!(self.table_type, TableType::Par) && self.readable_commits.contains(&commit_ts) {
            let _partition_write_guard = self.partition_guard.write();
            self.table.split_at_ts(commit_ts + 1).unwrap();
        }
        self.commit_tracker.publish(commit_ts);
        TxMetrics {
            latency_ms: start.elapsed().as_secs_f64() * 1000.0,
        }
    }
}

fn mean(values: &[f64]) -> f64 {
    if values.is_empty() {
        0.0
    } else {
        values.iter().sum::<f64>() / values.len() as f64
    }
}

fn percentile(values: &[f64], q: f64) -> f64 {
    if values.is_empty() {
        return 0.0;
    }
    let mut sorted = values.to_vec();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let idx = ((sorted.len() - 1) as f64 * q).round() as usize;
    sorted[idx.min(sorted.len() - 1)]
}

fn aggregate_trace(total_ms: f64, stats: WorkerStats) -> TraceResult {
    TraceResult {
        total_ms,
        avg_join_latency_ms: mean(&stats.join_latencies),
        p95_join_latency_ms: percentile(&stats.join_latencies, 0.95),
        avg_scan_latency_ms: mean(&stats.scan_latencies),
        p95_scan_latency_ms: percentile(&stats.scan_latencies, 0.95),
        avg_delta_latency_ms: mean(&stats.delta_latencies),
        avg_update_latency_ms: mean(&stats.update_latencies),
    }
}

fn create_trace_backend(
    cli: &Cli,
    parts: &[PartEntry],
    update_waves: &[Vec<UpdateOp>],
    readable_ts: &[Timestamp],
) -> Arc<dyn HtapTraceTable> {
    match cli.table_type {
        TableType::Snap => Arc::new(SnapTrace::new(parts, update_waves, cli.bucket_num)),
        TableType::Ivmh => Arc::new(IvmhTrace::new(parts, update_waves, cli.bucket_num)),
        TableType::Heap | TableType::Chain | TableType::Par => Arc::new(MvhtTrace::new(
            parts,
            cli.table_type,
            cli.repair_mode,
            cli.bucket_num,
            update_waves,
            readable_ts,
        )),
    }
}

fn run_trace_once(
    cli: &Cli,
    parts: &[PartEntry],
    probes: &[ProbeRow],
    base_updates: &[UpdateOp],
    trace: &[TraceTx],
    readable_ts: &[Timestamp],
) -> TraceResult {
    let update_waves = build_update_waves(base_updates, cli.update_waves);
    let backend = create_trace_backend(cli, parts, &update_waves, readable_ts);
    let trace = Arc::new(trace.to_vec());
    let probes = Arc::new(probes.to_vec());
    let barrier = Arc::new(Barrier::new(cli.worker_threads + 1));
    let next_idx = Arc::new(AtomicUsize::new(0));
    let mut handles = Vec::new();

    for _ in 0..cli.worker_threads {
        let backend = Arc::clone(&backend);
        let trace = Arc::clone(&trace);
        let probes = Arc::clone(&probes);
        let barrier = Arc::clone(&barrier);
        let next_idx = Arc::clone(&next_idx);
        handles.push(thread::spawn(move || {
            let mut stats = WorkerStats::default();
            barrier.wait();
            loop {
                let idx = next_idx.fetch_add(1, Ordering::Relaxed);
                if idx >= trace.len() {
                    break;
                }
                match &trace[idx] {
                    TraceTx::Read { shape, target_ts } => {
                        let metrics = match shape {
                            ReadShape::Join => backend.run_join_tx(*target_ts, &probes),
                            ReadShape::Scan => backend.run_scan_tx(*target_ts),
                        };
                        match shape {
                            ReadShape::Join => stats.join_latencies.push(metrics.latency_ms),
                            ReadShape::Scan => stats.scan_latencies.push(metrics.latency_ms),
                        }
                    }
                    TraceTx::Delta { from_ts, to_ts } => {
                        let metrics = backend.run_delta_tx(*from_ts, *to_ts);
                        stats.delta_latencies.push(metrics.latency_ms);
                    }
                    TraceTx::Update { wave_idx, commit_ts } => {
                        let metrics = backend.run_update_tx(*wave_idx, *commit_ts);
                        stats.update_latencies.push(metrics.latency_ms);
                    }
                }
            }
            stats
        }));
    }

    let start = Instant::now();
    barrier.wait();
    let mut merged = WorkerStats::default();
    for handle in handles {
        merged.merge(handle.join().unwrap());
    }
    let total_ms = start.elapsed().as_secs_f64() * 1000.0;
    aggregate_trace(total_ms, merged)
}

fn average_results(results: &[TraceResult]) -> TraceResult {
    let n = results.len() as f64;
    TraceResult {
        total_ms: results.iter().map(|r| r.total_ms).sum::<f64>() / n,
        avg_join_latency_ms: results.iter().map(|r| r.avg_join_latency_ms).sum::<f64>() / n,
        p95_join_latency_ms: results.iter().map(|r| r.p95_join_latency_ms).sum::<f64>() / n,
        avg_scan_latency_ms: results.iter().map(|r| r.avg_scan_latency_ms).sum::<f64>() / n,
        p95_scan_latency_ms: results.iter().map(|r| r.p95_scan_latency_ms).sum::<f64>() / n,
        avg_delta_latency_ms: results.iter().map(|r| r.avg_delta_latency_ms).sum::<f64>() / n,
        avg_update_latency_ms: results.iter().map(|r| r.avg_update_latency_ms).sum::<f64>() / n,
    }
}

fn write_csv(
    path: &str,
    cli: &Cli,
    update_ops: usize,
    result: &TraceResult,
) -> Result<(), Box<dyn Error>> {
    let exists = Path::new(path).exists() && metadata(path)?.len() > 0;
    let mut file = OpenOptions::new().create(true).append(true).open(path)?;
    if !exists {
        writeln!(
            file,
            "table_type,repair_mode,worker_threads,join_txs,scan_txs,delta_txs,update_waves,readable_every,historical_ratio,update_pct,bucket_num,update_ops,total_ms,avg_join_latency_ms,p95_join_latency_ms,avg_scan_latency_ms,p95_scan_latency_ms,avg_delta_latency_ms,avg_update_latency_ms"
        )?;
    }
    writeln!(
        file,
        "{:?},{:?},{},{},{},{},{},{},{:.4},{:.6},{},{},{:.6},{:.6},{:.6},{:.6},{:.6},{:.6},{:.6}",
        cli.table_type,
        cli.repair_mode,
        cli.worker_threads,
        cli.join_txs,
        cli.scan_txs,
        cli.delta_txs,
        cli.update_waves,
        cli.readable_every,
        cli.historical_ratio,
        cli.update_pct,
        cli.bucket_num,
        update_ops,
        result.total_ms,
        result.avg_join_latency_ms,
        result.p95_join_latency_ms,
        result.avg_scan_latency_ms,
        result.p95_scan_latency_ms,
        result.avg_delta_latency_ms,
        result.avg_update_latency_ms,
    )?;
    Ok(())
}

fn main() -> Result<(), Box<dyn Error>> {
    let cli = Cli::parse();
    let parts = read_part_table(&cli.part_file)?;
    let probes = read_probe_rows(&cli.lineitem_file)?;
    let updates = read_updates(&cli.updates_file)?;
    let (trace, readable_ts) = build_trace(
        cli.join_txs,
        cli.scan_txs,
        cli.delta_txs,
        cli.update_waves,
        cli.readable_every,
        cli.historical_ratio,
    );

    eprintln!("=== htap_trace_bench ===");
    eprintln!(
        "table={:?} repair={:?} workers={} join_txs={} scan_txs={} delta_txs={} update_waves={} readable_every={} hist_ratio={:.2}",
        cli.table_type,
        cli.repair_mode,
        cli.worker_threads,
        cli.join_txs,
        cli.scan_txs,
        cli.delta_txs,
        cli.update_waves,
        cli.readable_every,
        cli.historical_ratio,
    );
    eprintln!(
        "loaded: part_rows={} probe_rows={} update_ops_per_wave={} trace_len={} readable_ts={}",
        parts.len(),
        probes.len(),
        updates.len(),
        trace.len(),
        readable_ts.len(),
    );

    for w in 0..cli.warmup {
        let _ = run_trace_once(&cli, &parts, &probes, &updates, &trace, &readable_ts);
        eprintln!("  [warmup {} / {}] done", w + 1, cli.warmup);
    }

    let measured = cli.repeat.max(1);
    let mut results = Vec::with_capacity(measured);
    for i in 0..measured {
        let result = run_trace_once(&cli, &parts, &probes, &updates, &trace, &readable_ts);
        eprintln!(
            "  [iter {}] total={:.2}ms join_avg={:.2}ms scan_avg={:.2}ms delta_avg={:.2}ms update_avg={:.2}ms",
            i + 1,
            result.total_ms,
            result.avg_join_latency_ms,
            result.avg_scan_latency_ms,
            result.avg_delta_latency_ms,
            result.avg_update_latency_ms,
        );
        results.push(result);
    }

    results.sort_by(|a, b| a.total_ms.partial_cmp(&b.total_ms).unwrap());
    let trim = cli.trim.min(results.len() / 2);
    let trimmed = &results[trim..results.len() - trim];
    if trimmed.is_empty() {
        return Err("no results after trimming".into());
    }
    let avg = average_results(trimmed);

    eprintln!("=== Average (trimmed) ===");
    eprintln!("  total_ms: {:.3}", avg.total_ms);
    eprintln!("  avg_join_latency_ms: {:.3}", avg.avg_join_latency_ms);
    eprintln!("  avg_scan_latency_ms: {:.3}", avg.avg_scan_latency_ms);
    eprintln!("  avg_delta_latency_ms: {:.3}", avg.avg_delta_latency_ms);
    eprintln!("  avg_update_latency_ms: {:.3}", avg.avg_update_latency_ms);

    if let Some(ref path) = cli.output_csv {
        write_csv(path, &cli, updates.len(), &avg)?;
        eprintln!("  -> wrote {}", path);
    }

    Ok(())
}
