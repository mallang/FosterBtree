use clap::{Parser, ValueEnum};
use fbtree::bp::{get_in_mem_pool, ContainerKey, InMemPool, MemPool};
use fbtree::mvcc_index::dual_heap_hash::chained_hash_table::ChainedHashTable;
use fbtree::mvcc_index::hash_heap::hash_heap_table::HeapHashTable;
use fbtree::mvcc_index::hash_join_page::record::RecordRef;
use fbtree::mvcc_index::ts_partitioned::ts_partitioned_table::TsPartitionedTable;
use fbtree::mvcc_index::{MvccIndex, VersionsMap};
use fbtree::naive_hash_index::{HeapBaseMvccTable, NaiveHashTable};
use fbtree::prelude::{AccessMethodError, Timestamp};
use parking_lot::{Mutex, RwLock};
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::error::Error;
use std::fs::{metadata, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::ops::Range;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Instant;

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
#[command(about = "Concurrent read/update transaction benchmark over reusable hash state")]
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

    #[arg(long, default_value_t = 2048)]
    bucket_num: usize,

    #[arg(long, default_value_t = 4)]
    reader_threads: usize,

    #[arg(long, default_value_t = 1)]
    writer_threads: usize,

    #[arg(long, default_value_t = 1024)]
    read_tx_size: usize,

    #[arg(long, default_value_t = 256)]
    update_tx_size: usize,

    #[arg(long, default_value_t = 4)]
    read_rounds: usize,

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
    probes: u64,
}

#[derive(Clone, Copy, Default)]
struct UpdateTxMetrics {
    latency_ms: f64,
    wait_ms: f64,
    exec_ms: f64,
    updates: u64,
}

#[derive(Clone, Default)]
struct WorkerStats {
    read_latencies: Vec<f64>,
    read_waits: Vec<f64>,
    read_execs: Vec<f64>,
    update_latencies: Vec<f64>,
    update_waits: Vec<f64>,
    update_execs: Vec<f64>,
    total_probes: u64,
    total_updates: u64,
}

impl WorkerStats {
    fn record_read(&mut self, metrics: ReadTxMetrics) {
        self.read_latencies.push(metrics.latency_ms);
        self.read_waits.push(metrics.wait_ms);
        self.read_execs.push(metrics.exec_ms);
        self.total_probes += metrics.probes;
    }

    fn record_update(&mut self, metrics: UpdateTxMetrics) {
        self.update_latencies.push(metrics.latency_ms);
        self.update_waits.push(metrics.wait_ms);
        self.update_execs.push(metrics.exec_ms);
        self.total_updates += metrics.updates;
    }

    fn merge(&mut self, other: WorkerStats) {
        self.read_latencies.extend(other.read_latencies);
        self.read_waits.extend(other.read_waits);
        self.read_execs.extend(other.read_execs);
        self.update_latencies.extend(other.update_latencies);
        self.update_waits.extend(other.update_waits);
        self.update_execs.extend(other.update_execs);
        self.total_probes += other.total_probes;
        self.total_updates += other.total_updates;
    }
}

#[derive(Clone, Default)]
struct IterResult {
    total_ms: f64,
    total_probes: u64,
    total_updates: u64,
    read_tx_count: u64,
    update_tx_count: u64,
    avg_read_latency_ms: f64,
    p95_read_latency_ms: f64,
    avg_read_wait_ms: f64,
    p95_read_wait_ms: f64,
    avg_read_exec_ms: f64,
    avg_update_latency_ms: f64,
    p95_update_latency_ms: f64,
    avg_update_wait_ms: f64,
    p95_update_wait_ms: f64,
    avg_update_exec_ms: f64,
    reader_probe_throughput: f64,
    reader_tx_throughput: f64,
    writer_update_throughput: f64,
    writer_tx_throughput: f64,
}

type ProbeQueue = Arc<Mutex<VecDeque<Range<usize>>>>;
type UpdateQueue = Arc<Mutex<VecDeque<Range<usize>>>>;
type SnapshotMap = BTreeMap<Timestamp, Arc<NaiveHashTable<InMemPool>>>;
type CurrentRows = BTreeMap<Vec<u8>, Vec<u8>>;
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

trait ConcurrentTable: Send + Sync {
    fn run_read_tx(&self, probes: &[ProbeRow]) -> ReadTxMetrics;
    fn run_update_tx(&self, updates: &[UpdateOp]) -> UpdateTxMetrics;
}

struct SnapConcurrent {
    c_key: ContainerKey,
    mem_pool: Arc<InMemPool>,
    bucket_num: usize,
    base_table: HeapBaseMvccTable,
    current_rows: RwLock<CurrentRows>,
    snapshots: RwLock<SnapshotMap>,
    current_ts: AtomicU64,
    writer_gate: Mutex<()>,
}

impl SnapConcurrent {
    fn new(parts: &[PartEntry], bucket_num: usize) -> Self {
        let mem_pool = get_in_mem_pool();
        let c_key = ContainerKey::new(0, 0);
        let base_table = HeapBaseMvccTable::new();
        let mut current_rows = BTreeMap::new();
        for p in parts {
            base_table.insert_at_ts(&p.partkey, &p.partkey, &p.ptype, 1);
            current_rows.insert(p.partkey.clone(), p.ptype.clone());
        }
        let mut snapshots = BTreeMap::new();
        let initial = build_snapshot_from_rows(
            c_key,
            mem_pool.clone(),
            bucket_num,
            current_rows
                .iter()
                .map(|(k, v)| (k.as_slice(), k.as_slice(), v.as_slice())),
        );
        snapshots.insert(1, initial);
        Self {
            c_key,
            mem_pool,
            bucket_num,
            base_table,
            current_rows: RwLock::new(current_rows),
            snapshots: RwLock::new(snapshots),
            current_ts: AtomicU64::new(1),
            writer_gate: Mutex::new(()),
        }
    }
}

impl ConcurrentTable for SnapConcurrent {
    fn run_read_tx(&self, probes: &[ProbeRow]) -> ReadTxMetrics {
        let arrival = Instant::now();
        let ts = self.current_ts.load(Ordering::Acquire);
        let snapshot = {
            let tables = self.snapshots.read();
            tables.get(&ts).cloned().expect("snapshot missing for published ts")
        };
        let wait_ms = arrival.elapsed().as_secs_f64() * 1000.0;
        let exec_start = Instant::now();
        let probes_done = probe_naive_table(snapshot.as_ref(), probes);
        let exec_ms = exec_start.elapsed().as_secs_f64() * 1000.0;
        ReadTxMetrics {
            latency_ms: wait_ms + exec_ms,
            wait_ms,
            exec_ms,
            probes: probes_done,
        }
    }

    fn run_update_tx(&self, updates: &[UpdateOp]) -> UpdateTxMetrics {
        let arrival = Instant::now();
        let _writer = self.writer_gate.lock();
        let wait_ms = arrival.elapsed().as_secs_f64() * 1000.0;
        let new_ts = self.current_ts.load(Ordering::Acquire) + 1;
        for op in updates {
            self.base_table
                .update_at_ts(&op.partkey, &op.partkey, &op.new_ptype, new_ts);
        }
        {
            let mut rows = self.current_rows.write();
            for op in updates {
                rows.insert(op.partkey.clone(), op.new_ptype.clone());
            }
        }
        let exec_start = Instant::now();
        let snapshot = {
            let rows = self.current_rows.read();
            build_snapshot_from_rows(
                self.c_key,
                self.mem_pool.clone(),
                self.bucket_num,
                rows.iter()
                    .map(|(k, v)| (k.as_slice(), k.as_slice(), v.as_slice())),
            )
        };
        self.snapshots.write().insert(new_ts, snapshot);
        self.current_ts.store(new_ts, Ordering::Release);
        let exec_ms = exec_start.elapsed().as_secs_f64() * 1000.0;
        UpdateTxMetrics {
            latency_ms: wait_ms + exec_ms,
            wait_ms,
            exec_ms,
            updates: updates.len() as u64,
        }
    }
}

struct IvmhConcurrent {
    c_key: ContainerKey,
    mem_pool: Arc<InMemPool>,
    bucket_num: usize,
    base_table: HeapBaseMvccTable,
    current_rows: RwLock<CurrentRows>,
    current_table: RwLock<NaiveHashTable<InMemPool>>,
    snapshots: RwLock<SnapshotMap>,
    current_ts: AtomicU64,
    writer_gate: Mutex<()>,
}

impl IvmhConcurrent {
    fn new(parts: &[PartEntry], bucket_num: usize) -> Self {
        let mem_pool = get_in_mem_pool();
        let c_key = ContainerKey::new(0, 0);
        let base_table = HeapBaseMvccTable::new();
        let current_table = NaiveHashTable::new_with_bucket_num(c_key, mem_pool.clone(), bucket_num);
        let mut current_rows = BTreeMap::new();
        for p in parts {
            base_table.insert_at_ts(&p.partkey, &p.partkey, &p.ptype, 1);
            current_rows.insert(p.partkey.clone(), p.ptype.clone());
            current_table
                .insert(RecordRef::new(&p.partkey, &p.partkey, &p.ptype))
                .unwrap();
        }
        Self {
            c_key,
            mem_pool,
            bucket_num,
            base_table,
            current_rows: RwLock::new(current_rows),
            current_table: RwLock::new(current_table),
            snapshots: RwLock::new(BTreeMap::new()),
            current_ts: AtomicU64::new(1),
            writer_gate: Mutex::new(()),
        }
    }
}

impl ConcurrentTable for IvmhConcurrent {
    fn run_read_tx(&self, probes: &[ProbeRow]) -> ReadTxMetrics {
        let arrival = Instant::now();
        loop {
            let ts = self.current_ts.load(Ordering::Acquire);
            if let Some(snapshot) = self.snapshots.read().get(&ts).cloned() {
                let wait_ms = arrival.elapsed().as_secs_f64() * 1000.0;
                let exec_start = Instant::now();
                let probes_done = probe_naive_table(snapshot.as_ref(), probes);
                let exec_ms = exec_start.elapsed().as_secs_f64() * 1000.0;
                return ReadTxMetrics {
                    latency_ms: wait_ms + exec_ms,
                    wait_ms,
                    exec_ms,
                    probes: probes_done,
                };
            }

            let guard = self.current_table.read();
            if self.current_ts.load(Ordering::Acquire) != ts {
                drop(guard);
                thread::yield_now();
                continue;
            }

            let wait_ms = arrival.elapsed().as_secs_f64() * 1000.0;
            let exec_start = Instant::now();
            let probes_done = probe_naive_table(&guard, probes);
            let exec_ms = exec_start.elapsed().as_secs_f64() * 1000.0;
            return ReadTxMetrics {
                latency_ms: wait_ms + exec_ms,
                wait_ms,
                exec_ms,
                probes: probes_done,
            };
        }
    }

    fn run_update_tx(&self, updates: &[UpdateOp]) -> UpdateTxMetrics {
        let arrival = Instant::now();
        let _writer = self.writer_gate.lock();
        let wait_ms = arrival.elapsed().as_secs_f64() * 1000.0;
        let old_ts = self.current_ts.load(Ordering::Acquire);
        let new_ts = old_ts + 1;
        let mut exec_ms = 0.0;

        if !self.snapshots.read().contains_key(&old_ts) {
            let t = Instant::now();
            let snapshot = {
                let rows = self.current_rows.read();
                build_snapshot_from_rows(
                    self.c_key,
                    self.mem_pool.clone(),
                    self.bucket_num,
                    rows.iter()
                        .map(|(k, v)| (k.as_slice(), k.as_slice(), v.as_slice())),
                )
            };
            self.snapshots.write().insert(old_ts, snapshot);
            exec_ms += t.elapsed().as_secs_f64() * 1000.0;
        }

        {
            let mut rows = self.current_rows.write();
            for op in updates {
                self.base_table
                    .update_at_ts(&op.partkey, &op.partkey, &op.new_ptype, new_ts);
                rows.insert(op.partkey.clone(), op.new_ptype.clone());
            }
        }

        {
            let t = Instant::now();
            let current = self.current_table.write();
            for op in updates {
                current
                    .update(RecordRef::new(&op.partkey, &op.partkey, &op.new_ptype))
                    .unwrap();
            }
            exec_ms += t.elapsed().as_secs_f64() * 1000.0;
        }

        self.current_ts.store(new_ts, Ordering::Release);
        UpdateTxMetrics {
            latency_ms: wait_ms + exec_ms,
            wait_ms,
            exec_ms,
            updates: updates.len() as u64,
        }
    }
}

struct MvhtConcurrent {
    table: ArcMvccIndex,
    table_type: TableType,
    repair_mode: RepairMode,
    committed_ts: AtomicU64,
    writer_gate: Mutex<()>,
    partition_guard: RwLock<()>,
}

impl MvhtConcurrent {
    fn new(parts: &[PartEntry], table_type: TableType, repair_mode: RepairMode, bucket_num: usize) -> Self {
        let mem_pool = get_in_mem_pool();
        let c_key = ContainerKey::new(0, 0);
        let table: ArcMvccIndex = match table_type {
            TableType::Chain => Arc::new(ChainedHashTable::create_with_bucket_num(
                c_key,
                mem_pool.clone(),
                bucket_num,
            ).unwrap()),
            TableType::Heap => Arc::new(HeapHashTable::create_with_bucket_num(
                c_key,
                mem_pool.clone(),
                bucket_num,
            ).unwrap()),
            TableType::Par => Arc::new(TsPartitionedTable::create_with_bucket_num(
                c_key,
                mem_pool.clone(),
                bucket_num,
            ).unwrap()),
            _ => panic!("invalid MVHT table type"),
        };
        for (i, p) in parts.iter().enumerate() {
            table
                .insert(
                    p.partkey.clone(),
                    p.partkey.clone(),
                    1,
                    i as u64,
                    p.ptype.clone(),
                )
                .unwrap();
        }
        if matches!(table_type, TableType::Par) {
            table.split_at_ts(2).unwrap();
        }
        Self {
            table,
            table_type,
            repair_mode,
            committed_ts: AtomicU64::new(1),
            writer_gate: Mutex::new(()),
            partition_guard: RwLock::new(()),
        }
    }
}

impl ConcurrentTable for MvhtConcurrent {
    fn run_read_tx(&self, probes: &[ProbeRow]) -> ReadTxMetrics {
        let arrival = Instant::now();
        let read_ts = self.committed_ts.load(Ordering::Acquire);
        // TsPartitionedTable mutates partition metadata during split_at_ts().
        // Keep a shared guard for the duration of a read tx so splits cannot
        // race with scans.
        let _partition_read_guard = if matches!(self.table_type, TableType::Par) {
            Some(self.partition_guard.read())
        } else {
            None
        };
        let wait_ms = arrival.elapsed().as_secs_f64() * 1000.0;
        let exec_start = Instant::now();
        let mut probes_done = 0u64;
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
            probes_done += 1;
        }

        let exec_ms = exec_start.elapsed().as_secs_f64() * 1000.0;
        ReadTxMetrics {
            latency_ms: wait_ms + exec_ms,
            wait_ms,
            exec_ms,
            probes: probes_done,
        }
    }

    fn run_update_tx(&self, updates: &[UpdateOp]) -> UpdateTxMetrics {
        let arrival = Instant::now();
        let _writer = self.writer_gate.lock();
        let wait_ms = arrival.elapsed().as_secs_f64() * 1000.0;
        let exec_start = Instant::now();
        let new_ts = self.committed_ts.load(Ordering::Acquire) + 1;
        for op in updates {
            loop {
                let result = match self.repair_mode {
                    RepairMode::Wr => self.table.update_write_repair(
                        op.partkey.clone(),
                        op.partkey.clone(),
                        new_ts,
                        0,
                        op.new_ptype.clone(),
                    ),
                    _ => self.table.update(
                        op.partkey.clone(),
                        op.partkey.clone(),
                        new_ts,
                        0,
                        op.new_ptype.clone(),
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
            // split_at_ts mutates partition metadata and is not safe to run
            // concurrently with readers. Serialize it against read txs only.
            let _partition_write_guard = self.partition_guard.write();
            self.table.split_at_ts(new_ts + 1).unwrap();
        }
        self.committed_ts.store(new_ts, Ordering::Release);
        let exec_ms = exec_start.elapsed().as_secs_f64() * 1000.0;
        UpdateTxMetrics {
            latency_ms: wait_ms + exec_ms,
            wait_ms,
            exec_ms,
            updates: updates.len() as u64,
        }
    }
}

fn normalize_ptype_value(raw: &[u8]) -> Vec<u8> {
    const PTYPE_VALUE_SIZE: usize = 32;
    let mut out = vec![b' '; PTYPE_VALUE_SIZE];
    let n = raw.len().min(PTYPE_VALUE_SIZE);
    out[..n].copy_from_slice(&raw[..n]);
    out
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

fn probe_naive_table<T: MemPool + 'static>(table: &NaiveHashTable<T>, probes: &[ProbeRow]) -> u64 {
    let mut total = 0u64;
    let mut results = Vec::new();
    for row in probes {
        results.clear();
        table.scan_key_vec(&row.partkey, &mut results).unwrap();
        total += 1;
    }
    total
}

fn build_ranges(total_len: usize, chunk_size: usize, rounds: usize) -> VecDeque<Range<usize>> {
    let mut ranges = VecDeque::new();
    if total_len == 0 || chunk_size == 0 || rounds == 0 {
        return ranges;
    }
    for _ in 0..rounds {
        let mut start = 0usize;
        while start < total_len {
            let end = (start + chunk_size).min(total_len);
            ranges.push_back(start..end);
            start = end;
        }
    }
    ranges
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

fn create_benchmark(cli: &Cli, parts: &[PartEntry]) -> Arc<dyn ConcurrentTable> {
    match cli.table_type {
        TableType::Snap => Arc::new(SnapConcurrent::new(parts, cli.bucket_num)),
        TableType::Ivmh => Arc::new(IvmhConcurrent::new(parts, cli.bucket_num)),
        TableType::Heap | TableType::Chain | TableType::Par => Arc::new(MvhtConcurrent::new(
            parts,
            cli.table_type,
            cli.repair_mode,
            cli.bucket_num,
        )),
    }
}

fn run_once(
    cli: &Cli,
    parts: &[PartEntry],
    probe_rows: &[ProbeRow],
    updates: &[UpdateOp],
) -> IterResult {
    let bench = create_benchmark(cli, parts);
    let read_queue: ProbeQueue = Arc::new(Mutex::new(build_ranges(
        probe_rows.len(),
        cli.read_tx_size,
        cli.read_rounds,
    )));
    let update_queue: UpdateQueue = Arc::new(Mutex::new(build_ranges(
        updates.len(),
        cli.update_tx_size,
        1,
    )));

    let total_threads = cli.reader_threads + cli.writer_threads;
    let barrier = Arc::new(Barrier::new(total_threads + 1));
    let probe_rows = Arc::new(probe_rows.to_vec());
    let updates = Arc::new(updates.to_vec());
    let mut handles = Vec::new();

    for _ in 0..cli.reader_threads {
        let bench = Arc::clone(&bench);
        let queue = Arc::clone(&read_queue);
        let barrier = Arc::clone(&barrier);
        let rows = Arc::clone(&probe_rows);
        handles.push(thread::spawn(move || {
            let mut stats = WorkerStats::default();
            barrier.wait();
            loop {
                let range = {
                    let mut guard = queue.lock();
                    guard.pop_front()
                };
                let Some(range) = range else {
                    break;
                };
                let metrics = bench.run_read_tx(&rows[range]);
                stats.record_read(metrics);
            }
            stats
        }));
    }

    for _ in 0..cli.writer_threads {
        let bench = Arc::clone(&bench);
        let queue = Arc::clone(&update_queue);
        let barrier = Arc::clone(&barrier);
        let ops = Arc::clone(&updates);
        handles.push(thread::spawn(move || {
            let mut stats = WorkerStats::default();
            barrier.wait();
            loop {
                let range = {
                    let mut guard = queue.lock();
                    guard.pop_front()
                };
                let Some(range) = range else {
                    break;
                };
                let metrics = bench.run_update_tx(&ops[range]);
                stats.record_update(metrics);
            }
            stats
        }));
    }

    let global_start = Instant::now();
    barrier.wait();

    let mut merged = WorkerStats::default();
    for handle in handles {
        merged.merge(handle.join().unwrap());
    }

    let total_ms = global_start.elapsed().as_secs_f64() * 1000.0;
    let total_secs = total_ms / 1000.0;
    let read_tx_count = merged.read_latencies.len() as u64;
    let update_tx_count = merged.update_latencies.len() as u64;

    IterResult {
        total_ms,
        total_probes: merged.total_probes,
        total_updates: merged.total_updates,
        read_tx_count,
        update_tx_count,
        avg_read_latency_ms: mean(&merged.read_latencies),
        p95_read_latency_ms: percentile(&merged.read_latencies, 0.95),
        avg_read_wait_ms: mean(&merged.read_waits),
        p95_read_wait_ms: percentile(&merged.read_waits, 0.95),
        avg_read_exec_ms: mean(&merged.read_execs),
        avg_update_latency_ms: mean(&merged.update_latencies),
        p95_update_latency_ms: percentile(&merged.update_latencies, 0.95),
        avg_update_wait_ms: mean(&merged.update_waits),
        p95_update_wait_ms: percentile(&merged.update_waits, 0.95),
        avg_update_exec_ms: mean(&merged.update_execs),
        reader_probe_throughput: if total_secs > 0.0 {
            merged.total_probes as f64 / total_secs
        } else {
            0.0
        },
        reader_tx_throughput: if total_secs > 0.0 {
            read_tx_count as f64 / total_secs
        } else {
            0.0
        },
        writer_update_throughput: if total_secs > 0.0 {
            merged.total_updates as f64 / total_secs
        } else {
            0.0
        },
        writer_tx_throughput: if total_secs > 0.0 {
            update_tx_count as f64 / total_secs
        } else {
            0.0
        },
    }
}

fn average_results(results: &[IterResult]) -> IterResult {
    let n = results.len() as f64;
    let first = &results[0];
    IterResult {
        total_ms: results.iter().map(|r| r.total_ms).sum::<f64>() / n,
        total_probes: first.total_probes,
        total_updates: first.total_updates,
        read_tx_count: first.read_tx_count,
        update_tx_count: first.update_tx_count,
        avg_read_latency_ms: results.iter().map(|r| r.avg_read_latency_ms).sum::<f64>() / n,
        p95_read_latency_ms: results.iter().map(|r| r.p95_read_latency_ms).sum::<f64>() / n,
        avg_read_wait_ms: results.iter().map(|r| r.avg_read_wait_ms).sum::<f64>() / n,
        p95_read_wait_ms: results.iter().map(|r| r.p95_read_wait_ms).sum::<f64>() / n,
        avg_read_exec_ms: results.iter().map(|r| r.avg_read_exec_ms).sum::<f64>() / n,
        avg_update_latency_ms: results.iter().map(|r| r.avg_update_latency_ms).sum::<f64>() / n,
        p95_update_latency_ms: results.iter().map(|r| r.p95_update_latency_ms).sum::<f64>() / n,
        avg_update_wait_ms: results.iter().map(|r| r.avg_update_wait_ms).sum::<f64>() / n,
        p95_update_wait_ms: results.iter().map(|r| r.p95_update_wait_ms).sum::<f64>() / n,
        avg_update_exec_ms: results.iter().map(|r| r.avg_update_exec_ms).sum::<f64>() / n,
        reader_probe_throughput: results.iter().map(|r| r.reader_probe_throughput).sum::<f64>() / n,
        reader_tx_throughput: results.iter().map(|r| r.reader_tx_throughput).sum::<f64>() / n,
        writer_update_throughput: results.iter().map(|r| r.writer_update_throughput).sum::<f64>() / n,
        writer_tx_throughput: results.iter().map(|r| r.writer_tx_throughput).sum::<f64>() / n,
    }
}

fn write_csv(path: &str, cli: &Cli, result: &IterResult) -> Result<(), Box<dyn Error>> {
    let exists = Path::new(path).exists() && metadata(path)?.len() > 0;
    let mut file = OpenOptions::new().create(true).append(true).open(path)?;

    if !exists {
        writeln!(
            file,
            "table_type,repair_mode,reader_threads,writer_threads,read_tx_size,update_tx_size,read_rounds,update_pct,bucket_num,total_probes,total_updates,read_tx_count,update_tx_count,total_ms,avg_read_latency_ms,p95_read_latency_ms,avg_read_wait_ms,p95_read_wait_ms,avg_read_exec_ms,avg_update_latency_ms,p95_update_latency_ms,avg_update_wait_ms,p95_update_wait_ms,avg_update_exec_ms,reader_probe_throughput,reader_tx_throughput,writer_update_throughput,writer_tx_throughput"
        )?;
    }

    writeln!(
        file,
        "{:?},{:?},{},{},{},{},{},{:.6},{},{},{},{},{},{:.6},{:.6},{:.6},{:.6},{:.6},{:.6},{:.6},{:.6},{:.6},{:.6},{:.6},{:.6},{:.6},{:.6},{:.6}",
        cli.table_type,
        cli.repair_mode,
        cli.reader_threads,
        cli.writer_threads,
        cli.read_tx_size,
        cli.update_tx_size,
        cli.read_rounds,
        cli.update_pct,
        cli.bucket_num,
        result.total_probes,
        result.total_updates,
        result.read_tx_count,
        result.update_tx_count,
        result.total_ms,
        result.avg_read_latency_ms,
        result.p95_read_latency_ms,
        result.avg_read_wait_ms,
        result.p95_read_wait_ms,
        result.avg_read_exec_ms,
        result.avg_update_latency_ms,
        result.p95_update_latency_ms,
        result.avg_update_wait_ms,
        result.p95_update_wait_ms,
        result.avg_update_exec_ms,
        result.reader_probe_throughput,
        result.reader_tx_throughput,
        result.writer_update_throughput,
        result.writer_tx_throughput,
    )?;

    Ok(())
}

fn main() -> Result<(), Box<dyn Error>> {
    let cli = Cli::parse();

    eprintln!("=== concurrent_tx_bench ===");
    eprintln!(
        "table={:?} repair={:?} readers={} writers={} read_tx_size={} update_tx_size={} read_rounds={} buckets={}",
        cli.table_type,
        cli.repair_mode,
        cli.reader_threads,
        cli.writer_threads,
        cli.read_tx_size,
        cli.update_tx_size,
        cli.read_rounds,
        cli.bucket_num,
    );

    let parts = read_part_table(&cli.part_file)?;
    let probe_rows = read_probe_rows(&cli.lineitem_file)?;
    let update_ops = read_updates(&cli.updates_file)?;
    eprintln!(
        "loaded: part_rows={} probe_rows={} update_ops={} update_pct={:.4}",
        parts.len(),
        probe_rows.len(),
        update_ops.len(),
        cli.update_pct,
    );

    let read_tx_count = build_ranges(probe_rows.len(), cli.read_tx_size, cli.read_rounds).len();
    let update_tx_count = build_ranges(update_ops.len(), cli.update_tx_size, 1).len();
    eprintln!(
        "tx_shape: read_txs={} update_txs={}",
        read_tx_count,
        update_tx_count,
    );

    for w in 0..cli.warmup {
        let _ = run_once(&cli, &parts, &probe_rows, &update_ops);
        eprintln!("  [warmup {} / {}] done", w + 1, cli.warmup);
    }

    let measured = cli.repeat.max(1);
    let mut results = Vec::with_capacity(measured);
    for i in 0..measured {
        let result = run_once(&cli, &parts, &probe_rows, &update_ops);
        eprintln!(
            "  [iter {}] total={:.1}ms read_avg={:.2}ms read_p95={:.2}ms read_wait={:.2}ms upd_avg={:.2}ms read_probe_tput={:.0}/s read_tx_tput={:.1}/s",
            i + 1,
            result.total_ms,
            result.avg_read_latency_ms,
            result.p95_read_latency_ms,
            result.avg_read_wait_ms,
            result.avg_update_latency_ms,
            result.reader_probe_throughput,
            result.reader_tx_throughput,
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
    eprintln!("  total_ms:              {:.3}", avg.total_ms);
    eprintln!("  avg_read_latency_ms:   {:.3}", avg.avg_read_latency_ms);
    eprintln!("  p95_read_latency_ms:   {:.3}", avg.p95_read_latency_ms);
    eprintln!("  avg_read_wait_ms:      {:.3}", avg.avg_read_wait_ms);
    eprintln!("  avg_update_latency_ms: {:.3}", avg.avg_update_latency_ms);
    eprintln!("  reader_probe_tput:     {:.3}", avg.reader_probe_throughput);
    eprintln!("  reader_tx_tput:        {:.3}", avg.reader_tx_throughput);
    eprintln!("  writer_tx_tput:        {:.3}", avg.writer_tx_throughput);

    if let Some(ref path) = cli.output_csv {
        write_csv(path, &cli, &avg)?;
        eprintln!("  -> wrote {}", path);
    }

    Ok(())
}
