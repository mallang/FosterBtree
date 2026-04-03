use clap::{Parser, ValueEnum};
use fbtree::bp::{get_in_mem_pool, ContainerKey, InMemPool, MemPool};
use fbtree::mvcc_index::dual_heap_hash::chained_hash_table::ChainedHashTable;
use fbtree::mvcc_index::hash_heap::hash_heap_table::HeapHashTable;
use fbtree::mvcc_index::hash_join_page::record::RecordRef;
use fbtree::mvcc_index::ts_partitioned::ts_partitioned_table::TsPartitionedTable;
use fbtree::mvcc_index::{MvccIndex, VersionsMap};
use fbtree::naive_hash_index::{HeapBaseMvccTable, NaiveHashTable};
use fbtree::prelude::{AccessMethodError, Timestamp};
use parking_lot::{Condvar, Mutex, RwLock};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::error::Error;
use std::fs::{metadata, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::ops::Range;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Instant;

const INITIAL_TS: Timestamp = 1;
const UPDATED_TS: Timestamp = 2;

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
#[command(about = "Snapshot concurrency benchmark for old-snapshot and fresh reads")]
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

    #[arg(long, default_value_t = 4)]
    reader_threads: usize,

    #[arg(long, default_value_t = 2048)]
    read_tx_size: usize,

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

#[derive(Clone, Copy, Default)]
struct ReadTxMetrics {
    latency_ms: f64,
    wait_ms: f64,
    exec_ms: f64,
}

#[derive(Clone, Copy, Default)]
struct UpdateTxMetrics {
    latency_ms: f64,
    wait_ms: f64,
    exec_ms: f64,
}

#[derive(Clone, Default)]
struct WorkerStats {
    read_latencies: Vec<f64>,
    read_waits: Vec<f64>,
    read_execs: Vec<f64>,
    update_latencies: Vec<f64>,
    update_waits: Vec<f64>,
    update_execs: Vec<f64>,
}

impl WorkerStats {
    fn record_read(&mut self, metrics: ReadTxMetrics) {
        self.read_latencies.push(metrics.latency_ms);
        self.read_waits.push(metrics.wait_ms);
        self.read_execs.push(metrics.exec_ms);
    }

    fn record_update(&mut self, metrics: UpdateTxMetrics) {
        self.update_latencies.push(metrics.latency_ms);
        self.update_waits.push(metrics.wait_ms);
        self.update_execs.push(metrics.exec_ms);
    }

    fn merge(&mut self, other: WorkerStats) {
        self.read_latencies.extend(other.read_latencies);
        self.read_waits.extend(other.read_waits);
        self.read_execs.extend(other.read_execs);
        self.update_latencies.extend(other.update_latencies);
        self.update_waits.extend(other.update_waits);
        self.update_execs.extend(other.update_execs);
    }
}

#[derive(Clone, Default)]
struct CaseResult {
    total_ms: f64,
    avg_read_latency_ms: f64,
    p95_read_latency_ms: f64,
    avg_read_wait_ms: f64,
    p95_read_wait_ms: f64,
    avg_read_exec_ms: f64,
    avg_update_latency_ms: f64,
    avg_update_wait_ms: f64,
    avg_update_exec_ms: f64,
}

#[derive(Clone, Default)]
struct IterResult {
    history_total_ms: f64,
    history_avg_read_latency_ms: f64,
    history_p95_read_latency_ms: f64,
    history_avg_read_wait_ms: f64,
    history_p95_read_wait_ms: f64,
    history_avg_read_exec_ms: f64,
    history_avg_update_latency_ms: f64,
    fresh_total_ms: f64,
    fresh_avg_read_latency_ms: f64,
    fresh_p95_read_latency_ms: f64,
    fresh_avg_read_wait_ms: f64,
    fresh_p95_read_wait_ms: f64,
    fresh_avg_read_exec_ms: f64,
    fresh_avg_update_latency_ms: f64,
    mixed_total_ms: f64,
}

#[derive(Clone, Copy)]
enum ScenarioKind {
    Historical,
    Fresh,
}

#[derive(Clone, Copy)]
enum MixedReadKind {
    Historical,
    Fresh,
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

trait SnapshotConcurrencyTable: Send + Sync {
    fn run_historical_read_tx(&self, probes: &[ProbeRow]) -> ReadTxMetrics;
    fn run_fresh_read_tx(&self, probes: &[ProbeRow]) -> ReadTxMetrics;
    fn run_update_tx(&self, updates: &[UpdateOp]) -> UpdateTxMetrics;
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

struct SnapBlocking {
    c_key: ContainerKey,
    mem_pool: Arc<InMemPool>,
    bucket_num: usize,
    base_table: HeapBaseMvccTable,
    snapshots: RwLock<SnapshotMap>,
    build_state: Mutex<SnapshotBuildState>,
    build_cv: Condvar,
    current_ts: AtomicU64,
    writer_gate: Mutex<()>,
}

impl SnapBlocking {
    fn new(parts: &[PartEntry], updates: &[UpdateOp], bucket_num: usize) -> Self {
        let mem_pool = get_in_mem_pool();
        let c_key = ContainerKey::new(0, 0);
        let base_table = HeapBaseMvccTable::new();
        for part in parts {
            base_table.insert_at_ts(&part.partkey, &part.partkey, &part.ptype, INITIAL_TS);
        }
        for update in updates {
            base_table.update_at_ts(&update.partkey, &update.partkey, &update.new_ptype, UPDATED_TS);
        }

        Self {
            c_key,
            mem_pool,
            bucket_num,
            base_table,
            snapshots: RwLock::new(BTreeMap::new()),
            build_state: Mutex::new(SnapshotBuildState::default()),
            build_cv: Condvar::new(),
            current_ts: AtomicU64::new(INITIAL_TS),
            writer_gate: Mutex::new(()),
        }
    }

    fn build_snapshot_from_base(&self, ts: Timestamp) -> Arc<NaiveHashTable<InMemPool>> {
        build_snapshot_from_rows(
            self.c_key,
            self.mem_pool.clone(),
            self.bucket_num,
            self.base_table
                .scan_as_of(ts)
                .into_iter()
                .map(|(k, pk, v)| (k, pk, v)),
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

impl SnapshotConcurrencyTable for SnapBlocking {
    fn run_historical_read_tx(&self, probes: &[ProbeRow]) -> ReadTxMetrics {
        let arrival = Instant::now();
        let snapshot = self.get_or_build_snapshot(INITIAL_TS);
        let wait_ms = arrival.elapsed().as_secs_f64() * 1000.0;
        let exec_start = Instant::now();
        probe_naive_table(snapshot.as_ref(), probes);
        let exec_ms = exec_start.elapsed().as_secs_f64() * 1000.0;
        ReadTxMetrics {
            latency_ms: wait_ms + exec_ms,
            wait_ms,
            exec_ms,
        }
    }

    fn run_fresh_read_tx(&self, probes: &[ProbeRow]) -> ReadTxMetrics {
        let arrival = Instant::now();
        while self.current_ts.load(Ordering::Acquire) < UPDATED_TS {
            thread::yield_now();
        }
        let snapshot = self
            .snapshots
            .read()
            .get(&UPDATED_TS)
            .cloned()
            .expect("SNAP latest snapshot missing");
        let wait_ms = arrival.elapsed().as_secs_f64() * 1000.0;
        let exec_start = Instant::now();
        probe_naive_table(snapshot.as_ref(), probes);
        let exec_ms = exec_start.elapsed().as_secs_f64() * 1000.0;
        ReadTxMetrics {
            latency_ms: wait_ms + exec_ms,
            wait_ms,
            exec_ms,
        }
    }

    fn run_update_tx(&self, _updates: &[UpdateOp]) -> UpdateTxMetrics {
        let arrival = Instant::now();
        let _writer = self.writer_gate.lock();
        let wait_ms = arrival.elapsed().as_secs_f64() * 1000.0;
        let exec_start = Instant::now();
        let snapshot = self.build_snapshot_from_base(UPDATED_TS);
        self.snapshots.write().insert(UPDATED_TS, snapshot);
        self.current_ts.store(UPDATED_TS, Ordering::Release);
        let exec_ms = exec_start.elapsed().as_secs_f64() * 1000.0;
        UpdateTxMetrics {
            latency_ms: wait_ms + exec_ms,
            wait_ms,
            exec_ms,
        }
    }
}

struct IvmhBlocking {
    c_key: ContainerKey,
    mem_pool: Arc<InMemPool>,
    bucket_num: usize,
    base_table: HeapBaseMvccTable,
    current_table: RwLock<NaiveHashTable<InMemPool>>,
    snapshots: RwLock<SnapshotMap>,
    build_state: Mutex<SnapshotBuildState>,
    build_cv: Condvar,
    current_ts: AtomicU64,
    writer_gate: Mutex<()>,
}

impl IvmhBlocking {
    fn new(parts: &[PartEntry], updates: &[UpdateOp], bucket_num: usize) -> Self {
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
        for update in updates {
            base_table.update_at_ts(&update.partkey, &update.partkey, &update.new_ptype, UPDATED_TS);
        }

        Self {
            c_key,
            mem_pool,
            bucket_num,
            base_table,
            current_table: RwLock::new(current_table),
            snapshots: RwLock::new(BTreeMap::new()),
            build_state: Mutex::new(SnapshotBuildState::default()),
            build_cv: Condvar::new(),
            current_ts: AtomicU64::new(INITIAL_TS),
            writer_gate: Mutex::new(()),
        }
    }

    fn build_snapshot_from_base(&self, ts: Timestamp) -> Arc<NaiveHashTable<InMemPool>> {
        build_snapshot_from_rows(
            self.c_key,
            self.mem_pool.clone(),
            self.bucket_num,
            self.base_table
                .scan_as_of(ts)
                .into_iter()
                .map(|(k, pk, v)| (k, pk, v)),
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

impl SnapshotConcurrencyTable for IvmhBlocking {
    fn run_historical_read_tx(&self, probes: &[ProbeRow]) -> ReadTxMetrics {
        let arrival = Instant::now();
        let snapshot = self.get_or_build_snapshot(INITIAL_TS);
        let wait_ms = arrival.elapsed().as_secs_f64() * 1000.0;
        let exec_start = Instant::now();
        probe_naive_table(snapshot.as_ref(), probes);
        let exec_ms = exec_start.elapsed().as_secs_f64() * 1000.0;
        ReadTxMetrics {
            latency_ms: wait_ms + exec_ms,
            wait_ms,
            exec_ms,
        }
    }

    fn run_fresh_read_tx(&self, probes: &[ProbeRow]) -> ReadTxMetrics {
        let arrival = Instant::now();
        loop {
            if self.current_ts.load(Ordering::Acquire) < UPDATED_TS {
                thread::yield_now();
                continue;
            }
            let guard = self.current_table.read();
            if self.current_ts.load(Ordering::Acquire) < UPDATED_TS {
                drop(guard);
                thread::yield_now();
                continue;
            }
            let wait_ms = arrival.elapsed().as_secs_f64() * 1000.0;
            let exec_start = Instant::now();
            probe_naive_table(&guard, probes);
            let exec_ms = exec_start.elapsed().as_secs_f64() * 1000.0;
            return ReadTxMetrics {
                latency_ms: wait_ms + exec_ms,
                wait_ms,
                exec_ms,
            };
        }
    }

    fn run_update_tx(&self, updates: &[UpdateOp]) -> UpdateTxMetrics {
        let arrival = Instant::now();
        let _writer = self.writer_gate.lock();
        let wait_ms = arrival.elapsed().as_secs_f64() * 1000.0;
        let exec_start = Instant::now();
        let current = self.current_table.write();
        for update in updates {
            current
                .update(RecordRef::new(&update.partkey, &update.partkey, &update.new_ptype))
                .unwrap();
        }
        self.current_ts.store(UPDATED_TS, Ordering::Release);
        let exec_ms = exec_start.elapsed().as_secs_f64() * 1000.0;
        UpdateTxMetrics {
            latency_ms: wait_ms + exec_ms,
            wait_ms,
            exec_ms,
        }
    }
}

struct MvhtBlocking {
    table: ArcMvccIndex,
    table_type: TableType,
    repair_mode: RepairMode,
    committed_ts: AtomicU64,
    writer_gate: Mutex<()>,
    partition_guard: RwLock<()>,
}

impl MvhtBlocking {
    fn new(parts: &[PartEntry], table_type: TableType, repair_mode: RepairMode, bucket_num: usize) -> Self {
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
            _ => panic!("invalid MVHT table type"),
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
            table.split_at_ts(UPDATED_TS).unwrap();
        }
        Self {
            table,
            table_type,
            repair_mode,
            committed_ts: AtomicU64::new(INITIAL_TS),
            writer_gate: Mutex::new(()),
            partition_guard: RwLock::new(()),
        }
    }

    fn run_mvht_read_tx(&self, probes: &[ProbeRow], read_ts: Timestamp) -> ReadTxMetrics {
        let arrival = Instant::now();
        let _partition_read_guard = if matches!(self.table_type, TableType::Par) {
            Some(self.partition_guard.read())
        } else {
            None
        };
        let wait_ms = arrival.elapsed().as_secs_f64() * 1000.0;
        let exec_start = Instant::now();
        let mut nr_buf = HashMap::new();
        let mut rr_dedup = HashMap::new();
        let mut rr_versions: VersionsMap = HashMap::new();
        for row in probes {
            loop {
                let result = match self.repair_mode {
                    RepairMode::Nr => self.table.scan_key_vec_nr(&row.partkey, read_ts, &mut nr_buf),
                    RepairMode::Rr => self.table.scan_key_vec_rr(
                        &row.partkey,
                        read_ts,
                        &mut rr_dedup,
                        &mut rr_versions,
                    ),
                    RepairMode::Wr => self.table.scan_key_vec(&row.partkey, read_ts),
                };
                match result {
                    Ok(_) => break,
                    Err(AccessMethodError::PageReadLatchFailed | AccessMethodError::PageWriteLatchFailed) => {
                        thread::yield_now();
                    }
                    Err(err) => panic!("MVHT read failed: {}", err),
                }
            }
        }
        let exec_ms = exec_start.elapsed().as_secs_f64() * 1000.0;
        ReadTxMetrics {
            latency_ms: wait_ms + exec_ms,
            wait_ms,
            exec_ms,
        }
    }
}

impl SnapshotConcurrencyTable for MvhtBlocking {
    fn run_historical_read_tx(&self, probes: &[ProbeRow]) -> ReadTxMetrics {
        self.run_mvht_read_tx(probes, INITIAL_TS)
    }

    fn run_fresh_read_tx(&self, probes: &[ProbeRow]) -> ReadTxMetrics {
        let arrival = Instant::now();
        loop {
            if self.committed_ts.load(Ordering::Acquire) < UPDATED_TS {
                thread::yield_now();
                continue;
            }
            let _partition_read_guard = if matches!(self.table_type, TableType::Par) {
                Some(self.partition_guard.read())
            } else {
                None
            };
            if self.committed_ts.load(Ordering::Acquire) < UPDATED_TS {
                thread::yield_now();
                continue;
            }
            let wait_ms = arrival.elapsed().as_secs_f64() * 1000.0;
            let exec_start = Instant::now();
            let mut nr_buf = HashMap::new();
            let mut rr_dedup = HashMap::new();
            let mut rr_versions: VersionsMap = HashMap::new();
            for row in probes {
                loop {
                    let result = match self.repair_mode {
                        RepairMode::Nr => self.table.scan_key_vec_nr(&row.partkey, UPDATED_TS, &mut nr_buf),
                        RepairMode::Rr => self.table.scan_key_vec_rr(
                            &row.partkey,
                            UPDATED_TS,
                            &mut rr_dedup,
                            &mut rr_versions,
                        ),
                        RepairMode::Wr => self.table.scan_key_vec(&row.partkey, UPDATED_TS),
                    };
                    match result {
                        Ok(_) => break,
                        Err(AccessMethodError::PageReadLatchFailed | AccessMethodError::PageWriteLatchFailed) => {
                            thread::yield_now();
                        }
                        Err(err) => panic!("MVHT fresh read failed: {}", err),
                    }
                }
            }
            let exec_ms = exec_start.elapsed().as_secs_f64() * 1000.0;
            return ReadTxMetrics {
                latency_ms: wait_ms + exec_ms,
                wait_ms,
                exec_ms,
            };
        }
    }

    fn run_update_tx(&self, updates: &[UpdateOp]) -> UpdateTxMetrics {
        let arrival = Instant::now();
        let _writer = self.writer_gate.lock();
        let wait_ms = arrival.elapsed().as_secs_f64() * 1000.0;
        let exec_start = Instant::now();
        for update in updates {
            loop {
                let result = match self.repair_mode {
                    RepairMode::Wr => self.table.update_write_repair(
                        update.partkey.clone(),
                        update.partkey.clone(),
                        UPDATED_TS,
                        0,
                        update.new_ptype.clone(),
                    ),
                    _ => self.table.update(
                        update.partkey.clone(),
                        update.partkey.clone(),
                        UPDATED_TS,
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
        if matches!(self.table_type, TableType::Par) {
            let _partition_write_guard = self.partition_guard.write();
            self.table.split_at_ts(UPDATED_TS + 1).unwrap();
        }
        self.committed_ts.store(UPDATED_TS, Ordering::Release);
        let exec_ms = exec_start.elapsed().as_secs_f64() * 1000.0;
        UpdateTxMetrics {
            latency_ms: wait_ms + exec_ms,
            wait_ms,
            exec_ms,
        }
    }
}

fn probe_naive_table<T: MemPool + 'static>(table: &NaiveHashTable<T>, probes: &[ProbeRow]) {
    let mut results = Vec::new();
    for row in probes {
        results.clear();
        table.scan_key_vec(&row.partkey, &mut results).unwrap();
    }
}

fn build_reader_ranges(total_len: usize, chunk_size: usize, readers: usize) -> Vec<Range<usize>> {
    let needed = chunk_size * readers;
    assert!(
        total_len >= needed,
        "not enough probe rows: need {}, have {}",
        needed,
        total_len
    );
    (0..readers)
        .map(|i| {
            let start = i * chunk_size;
            let end = start + chunk_size;
            start..end
        })
        .collect()
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

fn aggregate_case(total_ms: f64, stats: WorkerStats) -> CaseResult {
    CaseResult {
        total_ms,
        avg_read_latency_ms: mean(&stats.read_latencies),
        p95_read_latency_ms: percentile(&stats.read_latencies, 0.95),
        avg_read_wait_ms: mean(&stats.read_waits),
        p95_read_wait_ms: percentile(&stats.read_waits, 0.95),
        avg_read_exec_ms: mean(&stats.read_execs),
        avg_update_latency_ms: mean(&stats.update_latencies),
        avg_update_wait_ms: mean(&stats.update_waits),
        avg_update_exec_ms: mean(&stats.update_execs),
    }
}

fn create_benchmark(
    cli: &Cli,
    parts: &[PartEntry],
    updates: &[UpdateOp],
) -> Arc<dyn SnapshotConcurrencyTable> {
    match cli.table_type {
        TableType::Snap => Arc::new(SnapBlocking::new(parts, updates, cli.bucket_num)),
        TableType::Ivmh => Arc::new(IvmhBlocking::new(parts, updates, cli.bucket_num)),
        TableType::Heap | TableType::Chain | TableType::Par => Arc::new(MvhtBlocking::new(
            parts,
            cli.table_type,
            cli.repair_mode,
            cli.bucket_num,
        )),
    }
}

fn run_case(
    cli: &Cli,
    parts: &[PartEntry],
    probe_rows: &[ProbeRow],
    updates: &[UpdateOp],
    scenario: ScenarioKind,
) -> CaseResult {
    let bench = create_benchmark(cli, parts, updates);
    let ranges = build_reader_ranges(probe_rows.len(), cli.read_tx_size, cli.reader_threads);
    let barrier = Arc::new(Barrier::new(cli.reader_threads + 2));
    let probe_rows = Arc::new(probe_rows.to_vec());
    let updates = Arc::new(updates.to_vec());
    let mut handles = Vec::new();

    for range in ranges {
        let bench = Arc::clone(&bench);
        let barrier = Arc::clone(&barrier);
        let probe_rows = Arc::clone(&probe_rows);
        handles.push(thread::spawn(move || {
            let mut stats = WorkerStats::default();
            barrier.wait();
            let metrics = match scenario {
                ScenarioKind::Historical => bench.run_historical_read_tx(&probe_rows[range]),
                ScenarioKind::Fresh => bench.run_fresh_read_tx(&probe_rows[range]),
            };
            stats.record_read(metrics);
            stats
        }));
    }

    {
        let bench = Arc::clone(&bench);
        let barrier = Arc::clone(&barrier);
        let updates = Arc::clone(&updates);
        handles.push(thread::spawn(move || {
            let mut stats = WorkerStats::default();
            barrier.wait();
            let metrics = bench.run_update_tx(&updates);
            stats.record_update(metrics);
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
    aggregate_case(total_ms, merged)
}

fn run_mixed_case(
    cli: &Cli,
    parts: &[PartEntry],
    probe_rows: &[ProbeRow],
    updates: &[UpdateOp],
) -> CaseResult {
    let bench = create_benchmark(cli, parts, updates);
    let ranges = build_reader_ranges(probe_rows.len(), cli.read_tx_size, cli.reader_threads * 2);
    let barrier = Arc::new(Barrier::new(cli.reader_threads + 2));
    let probe_rows = Arc::new(probe_rows.to_vec());
    let updates = Arc::new(updates.to_vec());
    let mut handles = Vec::new();

    for worker_idx in 0..cli.reader_threads {
        let bench = Arc::clone(&bench);
        let barrier = Arc::clone(&barrier);
        let probe_rows = Arc::clone(&probe_rows);
        let first = ranges[2 * worker_idx].clone();
        let second = ranges[2 * worker_idx + 1].clone();
        let schedule = if worker_idx % 2 == 0 {
            [
                (MixedReadKind::Historical, first),
                (MixedReadKind::Fresh, second),
            ]
        } else {
            [
                (MixedReadKind::Fresh, first),
                (MixedReadKind::Historical, second),
            ]
        };
        handles.push(thread::spawn(move || {
            let mut stats = WorkerStats::default();
            barrier.wait();
            for (kind, range) in schedule {
                let metrics = match kind {
                    MixedReadKind::Historical => bench.run_historical_read_tx(&probe_rows[range]),
                    MixedReadKind::Fresh => bench.run_fresh_read_tx(&probe_rows[range]),
                };
                stats.record_read(metrics);
            }
            stats
        }));
    }

    {
        let bench = Arc::clone(&bench);
        let barrier = Arc::clone(&barrier);
        let updates = Arc::clone(&updates);
        handles.push(thread::spawn(move || {
            let mut stats = WorkerStats::default();
            barrier.wait();
            let metrics = bench.run_update_tx(&updates);
            stats.record_update(metrics);
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
    aggregate_case(total_ms, merged)
}

fn run_once(cli: &Cli, parts: &[PartEntry], probes: &[ProbeRow], updates: &[UpdateOp]) -> IterResult {
    let history = run_case(cli, parts, probes, updates, ScenarioKind::Historical);
    let fresh = run_case(cli, parts, probes, updates, ScenarioKind::Fresh);
    let mixed = run_mixed_case(cli, parts, probes, updates);
    IterResult {
        history_total_ms: history.total_ms,
        history_avg_read_latency_ms: history.avg_read_latency_ms,
        history_p95_read_latency_ms: history.p95_read_latency_ms,
        history_avg_read_wait_ms: history.avg_read_wait_ms,
        history_p95_read_wait_ms: history.p95_read_wait_ms,
        history_avg_read_exec_ms: history.avg_read_exec_ms,
        history_avg_update_latency_ms: history.avg_update_latency_ms,
        fresh_total_ms: fresh.total_ms,
        fresh_avg_read_latency_ms: fresh.avg_read_latency_ms,
        fresh_p95_read_latency_ms: fresh.p95_read_latency_ms,
        fresh_avg_read_wait_ms: fresh.avg_read_wait_ms,
        fresh_p95_read_wait_ms: fresh.p95_read_wait_ms,
        fresh_avg_read_exec_ms: fresh.avg_read_exec_ms,
        fresh_avg_update_latency_ms: fresh.avg_update_latency_ms,
        mixed_total_ms: mixed.total_ms,
    }
}

fn average_results(results: &[IterResult]) -> IterResult {
    let n = results.len() as f64;
    IterResult {
        history_total_ms: results.iter().map(|r| r.history_total_ms).sum::<f64>() / n,
        history_avg_read_latency_ms: results.iter().map(|r| r.history_avg_read_latency_ms).sum::<f64>() / n,
        history_p95_read_latency_ms: results.iter().map(|r| r.history_p95_read_latency_ms).sum::<f64>() / n,
        history_avg_read_wait_ms: results.iter().map(|r| r.history_avg_read_wait_ms).sum::<f64>() / n,
        history_p95_read_wait_ms: results.iter().map(|r| r.history_p95_read_wait_ms).sum::<f64>() / n,
        history_avg_read_exec_ms: results.iter().map(|r| r.history_avg_read_exec_ms).sum::<f64>() / n,
        history_avg_update_latency_ms: results.iter().map(|r| r.history_avg_update_latency_ms).sum::<f64>() / n,
        fresh_total_ms: results.iter().map(|r| r.fresh_total_ms).sum::<f64>() / n,
        fresh_avg_read_latency_ms: results.iter().map(|r| r.fresh_avg_read_latency_ms).sum::<f64>() / n,
        fresh_p95_read_latency_ms: results.iter().map(|r| r.fresh_p95_read_latency_ms).sum::<f64>() / n,
        fresh_avg_read_wait_ms: results.iter().map(|r| r.fresh_avg_read_wait_ms).sum::<f64>() / n,
        fresh_p95_read_wait_ms: results.iter().map(|r| r.fresh_p95_read_wait_ms).sum::<f64>() / n,
        fresh_avg_read_exec_ms: results.iter().map(|r| r.fresh_avg_read_exec_ms).sum::<f64>() / n,
        fresh_avg_update_latency_ms: results.iter().map(|r| r.fresh_avg_update_latency_ms).sum::<f64>() / n,
        mixed_total_ms: results.iter().map(|r| r.mixed_total_ms).sum::<f64>() / n,
    }
}

fn write_csv(path: &str, cli: &Cli, update_ops: usize, result: &IterResult) -> Result<(), Box<dyn Error>> {
    let exists = Path::new(path).exists() && metadata(path)?.len() > 0;
    let mut file = OpenOptions::new().create(true).append(true).open(path)?;
    if !exists {
        writeln!(
            file,
            "table_type,repair_mode,reader_threads,read_tx_size,update_pct,bucket_num,update_ops,\
history_total_ms,history_avg_read_latency_ms,history_p95_read_latency_ms,history_avg_read_wait_ms,\
history_p95_read_wait_ms,history_avg_read_exec_ms,history_avg_update_latency_ms,\
fresh_total_ms,fresh_avg_read_latency_ms,fresh_p95_read_latency_ms,fresh_avg_read_wait_ms,\
fresh_p95_read_wait_ms,fresh_avg_read_exec_ms,fresh_avg_update_latency_ms,mixed_total_ms"
        )?;
    }
    writeln!(
        file,
        "{:?},{:?},{},{},{:.6},{},{},{:.6},{:.6},{:.6},{:.6},{:.6},{:.6},{:.6},{:.6},{:.6},{:.6},{:.6},{:.6},{:.6},{:.6},{:.6}",
        cli.table_type,
        cli.repair_mode,
        cli.reader_threads,
        cli.read_tx_size,
        cli.update_pct,
        cli.bucket_num,
        update_ops,
        result.history_total_ms,
        result.history_avg_read_latency_ms,
        result.history_p95_read_latency_ms,
        result.history_avg_read_wait_ms,
        result.history_p95_read_wait_ms,
        result.history_avg_read_exec_ms,
        result.history_avg_update_latency_ms,
        result.fresh_total_ms,
        result.fresh_avg_read_latency_ms,
        result.fresh_p95_read_latency_ms,
        result.fresh_avg_read_wait_ms,
        result.fresh_p95_read_wait_ms,
        result.fresh_avg_read_exec_ms,
        result.fresh_avg_update_latency_ms,
        result.mixed_total_ms,
    )?;
    Ok(())
}

fn main() -> Result<(), Box<dyn Error>> {
    let cli = Cli::parse();

    eprintln!("=== snapshot_concurrency_bench ===");
    eprintln!(
        "table={:?} repair={:?} readers={} read_tx_size={} buckets={}",
        cli.table_type,
        cli.repair_mode,
        cli.reader_threads,
        cli.read_tx_size,
        cli.bucket_num,
    );

    let parts = read_part_table(&cli.part_file)?;
    let probes = read_probe_rows(&cli.lineitem_file)?;
    let updates = read_updates(&cli.updates_file)?;
    eprintln!(
        "loaded: part_rows={} probe_rows={} update_ops={} update_pct={:.4}",
        parts.len(),
        probes.len(),
        updates.len(),
        cli.update_pct,
    );

    for w in 0..cli.warmup {
        let _ = run_once(&cli, &parts, &probes, &updates);
        eprintln!("  [warmup {} / {}] done", w + 1, cli.warmup);
    }

    let measured = cli.repeat.max(1);
    let mut results = Vec::with_capacity(measured);
    for i in 0..measured {
        let result = run_once(&cli, &parts, &probes, &updates);
        eprintln!(
            "  [iter {}] hist_wait={:.2}ms hist_lat={:.2}ms fresh_wait={:.2}ms fresh_lat={:.2}ms mixed_total={:.2}ms",
            i + 1,
            result.history_avg_read_wait_ms,
            result.history_avg_read_latency_ms,
            result.fresh_avg_read_wait_ms,
            result.fresh_avg_read_latency_ms,
            result.mixed_total_ms,
        );
        results.push(result);
    }

    results.sort_by(|a, b| {
        let ta = a.history_total_ms + a.fresh_total_ms + a.mixed_total_ms;
        let tb = b.history_total_ms + b.fresh_total_ms + b.mixed_total_ms;
        ta.partial_cmp(&tb).unwrap()
    });
    let trim = cli.trim.min(results.len() / 2);
    let trimmed = &results[trim..results.len() - trim];
    if trimmed.is_empty() {
        return Err("no results after trimming".into());
    }
    let avg = average_results(trimmed);

    eprintln!("=== Average (trimmed) ===");
    eprintln!("  history_avg_read_wait_ms: {:.3}", avg.history_avg_read_wait_ms);
    eprintln!("  history_avg_read_latency_ms: {:.3}", avg.history_avg_read_latency_ms);
    eprintln!("  fresh_avg_read_wait_ms: {:.3}", avg.fresh_avg_read_wait_ms);
    eprintln!("  fresh_avg_read_latency_ms: {:.3}", avg.fresh_avg_read_latency_ms);
    eprintln!("  mixed_total_ms: {:.3}", avg.mixed_total_ms);

    if let Some(ref path) = cli.output_csv {
        write_csv(path, &cli, updates.len(), &avg)?;
        eprintln!("  -> wrote {}", path);
    }

    Ok(())
}
