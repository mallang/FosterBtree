use std::{path::PathBuf, sync::Arc, time::Instant};

use clap::{Parser, ValueEnum};
use fbtree::{
    access_method::{
        chain::prelude::HashReadOptimize,
        fbt::prelude::FosterBtree,
        paged_hash_chain_v1::prelude::{encode_hash_key, PagedHashChainV1},
        UniqueKeyIndex,
    },
    bp::{get_in_mem_pool, ContainerKey, InMemPool},
    prelude::PAGE_SIZE,
    ycsb::prelude::{
        make_lookup_trace, read_lookup_trace, write_lookup_trace, ycsb_key_bytes, YcsbDistribution,
        YcsbLookupTrace, YcsbLookupTraceConfig,
    },
};
use hdrhistogram::Histogram;

#[derive(Debug, Parser)]
#[command(about = "V0 point-lookup benchmark for PagedHashChain vs HashBTree")]
struct Args {
    #[arg(long, value_enum, default_value = "all")]
    variant: Variant,

    #[arg(long, default_value_t = 10_000)]
    n_keys: usize,

    #[arg(long, default_value_t = 8)]
    key_size: usize,

    #[arg(long, default_value_t = 100_000)]
    lookups_per_thread: usize,

    #[arg(long, default_value_t = 1_000)]
    warmup_lookups_per_thread: usize,

    #[arg(long, default_value_t = 1)]
    threads: usize,

    #[arg(long, value_enum, default_value = "uniform")]
    distribution: LookupDistribution,

    #[arg(long, default_value_t = 0.8)]
    zipf_theta: f64,

    #[arg(long, default_value_t = 4_096)]
    buckets: usize,

    #[arg(long, default_value_t = 8)]
    value_size: usize,

    #[arg(long, default_value_t = 42)]
    seed: u64,

    #[arg(long)]
    workload_in: Option<PathBuf>,

    #[arg(long)]
    workload_out: Option<PathBuf>,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum Variant {
    All,
    #[value(name = "paged-hash-chain")]
    PagedHashChain,
    #[value(name = "paged-hash-chain-v1")]
    PagedHashChainV1,
    #[value(name = "hash-btree")]
    HashBTree,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
enum LookupDistribution {
    Uniform,
    Zipf,
}

trait PointIndex: Send + Sync {
    fn name(&self) -> &'static str;
    fn insert_point(&self, key: &[u8], value: &[u8]);
    fn get_point(&self, key: &[u8]) -> Vec<u8>;
}

impl PointIndex for HashReadOptimize<InMemPool> {
    fn name(&self) -> &'static str {
        "paged_hash_chain"
    }

    fn insert_point(&self, key: &[u8], value: &[u8]) {
        UniqueKeyIndex::insert(self, key, value).unwrap();
    }

    fn get_point(&self, key: &[u8]) -> Vec<u8> {
        UniqueKeyIndex::get(self, key).unwrap()
    }
}

impl PointIndex for PagedHashChainV1<InMemPool> {
    fn name(&self) -> &'static str {
        "paged_hash_chain_v1"
    }

    fn insert_point(&self, key: &[u8], value: &[u8]) {
        UniqueKeyIndex::insert(self, key, value).unwrap();
    }

    fn get_point(&self, key: &[u8]) -> Vec<u8> {
        UniqueKeyIndex::get(self, key).unwrap()
    }
}

struct HashBTree {
    tree: Arc<FosterBtree<InMemPool>>,
}

impl HashBTree {
    fn new(tree: Arc<FosterBtree<InMemPool>>) -> Self {
        Self { tree }
    }

    fn encode_key(key: &[u8]) -> Vec<u8> {
        encode_hash_key(key)
    }
}

impl PointIndex for HashBTree {
    fn name(&self) -> &'static str {
        "hash_btree"
    }

    fn insert_point(&self, key: &[u8], value: &[u8]) {
        let encoded_key = Self::encode_key(key);
        UniqueKeyIndex::insert(self.tree.as_ref(), &encoded_key, value).unwrap();
    }

    fn get_point(&self, key: &[u8]) -> Vec<u8> {
        let encoded_key = Self::encode_key(key);
        UniqueKeyIndex::get(self.tree.as_ref(), &encoded_key).unwrap()
    }
}

struct BenchResult {
    variant: &'static str,
    n_keys: usize,
    distribution: YcsbDistribution,
    zipf_theta: f64,
    threads: usize,
    measured_lookups: usize,
    build_sec: f64,
    duration_sec: f64,
    throughput_ops_sec: f64,
    avg_latency_ns: f64,
    p50_latency_ns: u64,
    p95_latency_ns: u64,
    p99_latency_ns: u64,
}

struct ThreadResult {
    count: usize,
    total_latency_ns: u128,
    histogram: Histogram<u64>,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    validate_args(&args)?;
    let trace = load_or_make_trace(&args)?;

    println!(
        "variant,n_keys,key_size,page_size,distribution,zipf_theta,threads,measured_lookups,build_sec,duration_sec,throughput_ops_sec,avg_latency_ns,p50_latency_ns,p95_latency_ns,p99_latency_ns,bucket_count"
    );

    for variant in variants_to_run(args.variant) {
        let result = run_variant(&args, &trace, variant)?;
        println!(
            "{},{},{},{},{},{:.3},{},{},{:.6},{:.6},{:.3},{:.1},{},{},{},{}",
            result.variant,
            result.n_keys,
            trace.config.key_size,
            PAGE_SIZE,
            result.distribution.as_str(),
            result.zipf_theta,
            result.threads,
            result.measured_lookups,
            result.build_sec,
            result.duration_sec,
            result.throughput_ops_sec,
            result.avg_latency_ns,
            result.p50_latency_ns,
            result.p95_latency_ns,
            result.p99_latency_ns,
            args.buckets,
        );
    }

    Ok(())
}

fn validate_args(args: &Args) -> Result<(), Box<dyn std::error::Error>> {
    if args.value_size == 0 {
        return Err("--value-size must be greater than zero".into());
    }
    if args.buckets == 0 {
        return Err("--buckets must be greater than zero".into());
    }
    if args.workload_in.is_some() && args.workload_out.is_some() {
        return Err("--workload-in and --workload-out are mutually exclusive".into());
    }
    if args.workload_in.is_none() {
        make_trace_config(args).validate()?;
    }
    Ok(())
}

fn variants_to_run(variant: Variant) -> Vec<Variant> {
    match variant {
        Variant::All => vec![
            Variant::PagedHashChain,
            Variant::PagedHashChainV1,
            Variant::HashBTree,
        ],
        other => vec![other],
    }
}

fn load_or_make_trace(args: &Args) -> Result<YcsbLookupTrace, Box<dyn std::error::Error>> {
    if let Some(path) = &args.workload_in {
        let trace = read_lookup_trace(path)?;
        eprintln!(
            "loaded workload={} n_keys={} key_size={} distribution={} theta={} threads={} warmup/thread={} lookups/thread={}",
            path.display(),
            trace.config.n_keys,
            trace.config.key_size,
            trace.config.distribution.as_str(),
            trace.config.zipf_theta,
            trace.config.threads,
            trace.config.warmup_lookups_per_thread,
            trace.config.lookups_per_thread
        );
        return Ok(trace);
    }

    let trace = make_lookup_trace(make_trace_config(args))?;
    if let Some(path) = &args.workload_out {
        write_lookup_trace(path, &trace)?;
        eprintln!(
            "wrote workload={} n_keys={} key_size={} distribution={} theta={} threads={} warmup/thread={} lookups/thread={}",
            path.display(),
            trace.config.n_keys,
            trace.config.key_size,
            trace.config.distribution.as_str(),
            trace.config.zipf_theta,
            trace.config.threads,
            trace.config.warmup_lookups_per_thread,
            trace.config.lookups_per_thread
        );
    }
    Ok(trace)
}

fn make_trace_config(args: &Args) -> YcsbLookupTraceConfig {
    YcsbLookupTraceConfig {
        n_keys: args.n_keys,
        key_size: args.key_size,
        lookups_per_thread: args.lookups_per_thread,
        warmup_lookups_per_thread: args.warmup_lookups_per_thread,
        threads: args.threads,
        distribution: args.distribution.into(),
        zipf_theta: args.zipf_theta,
        seed: args.seed,
    }
}

fn run_variant(
    args: &Args,
    trace: &YcsbLookupTrace,
    variant: Variant,
) -> Result<BenchResult, Box<dyn std::error::Error>> {
    let index = make_index(variant, args.buckets);
    let payload = vec![7_u8; args.value_size];

    eprintln!(
        "building variant={} n_keys={} key_size={} value_size={} page_size={}",
        index.name(),
        trace.config.n_keys,
        trace.config.key_size,
        args.value_size,
        PAGE_SIZE
    );
    let build_start = Instant::now();
    build_index(
        index.as_ref(),
        trace.config.n_keys,
        trace.config.key_size,
        &payload,
    );
    let build_sec = build_start.elapsed().as_secs_f64();

    eprintln!(
        "running variant={} distribution={:?} theta={} threads={} lookups/thread={}",
        index.name(),
        trace.config.distribution,
        trace.config.zipf_theta,
        trace.config.threads,
        trace.config.lookups_per_thread
    );

    run_warmup(
        index.clone(),
        trace.config.key_size,
        &payload,
        &trace.warmup,
    );

    let run_start = Instant::now();
    let thread_results = run_measured(
        index.clone(),
        trace.config.key_size,
        &payload,
        &trace.measured,
    )?;
    let duration_sec = run_start.elapsed().as_secs_f64();

    let mut total_count = 0usize;
    let mut total_latency_ns = 0u128;
    let mut histogram = Histogram::<u64>::new(3)?;
    for result in thread_results {
        total_count += result.count;
        total_latency_ns += result.total_latency_ns;
        histogram.add(&result.histogram)?;
    }

    Ok(BenchResult {
        variant: index.name(),
        n_keys: trace.config.n_keys,
        distribution: trace.config.distribution,
        zipf_theta: trace.config.zipf_theta,
        threads: trace.config.threads,
        measured_lookups: total_count,
        build_sec,
        duration_sec,
        throughput_ops_sec: total_count as f64 / duration_sec,
        avg_latency_ns: total_latency_ns as f64 / total_count as f64,
        p50_latency_ns: histogram.value_at_quantile(0.50),
        p95_latency_ns: histogram.value_at_quantile(0.95),
        p99_latency_ns: histogram.value_at_quantile(0.99),
    })
}

fn make_index(variant: Variant, buckets: usize) -> Arc<dyn PointIndex> {
    let mem_pool = get_in_mem_pool();
    let c_key = ContainerKey::new(0, 0);
    match variant {
        Variant::PagedHashChain => Arc::new(HashReadOptimize::new(c_key, mem_pool, buckets)),
        Variant::PagedHashChainV1 => Arc::new(PagedHashChainV1::new(c_key, mem_pool, buckets)),
        Variant::HashBTree => {
            let tree = Arc::new(FosterBtree::new(c_key, mem_pool));
            Arc::new(HashBTree::new(tree))
        }
        Variant::All => unreachable!("expanded by variants_to_run"),
    }
}

fn build_index(index: &dyn PointIndex, n_keys: usize, key_size: usize, payload: &[u8]) {
    for key_id in 0..n_keys {
        let key = ycsb_key_bytes(key_id, key_size);
        index.insert_point(&key, payload);
    }
}

fn run_warmup(
    index: Arc<dyn PointIndex>,
    key_size: usize,
    payload: &[u8],
    requests: &[Vec<usize>],
) {
    std::thread::scope(|scope| {
        for thread_requests in requests {
            let index = index.clone();
            scope.spawn(move || {
                for &key_id in thread_requests {
                    let key = ycsb_key_bytes(key_id, key_size);
                    let value = index.get_point(&key);
                    debug_assert_eq!(value, payload);
                }
            });
        }
    });
}

fn run_measured(
    index: Arc<dyn PointIndex>,
    key_size: usize,
    payload: &[u8],
    requests: &[Vec<usize>],
) -> Result<Vec<ThreadResult>, Box<dyn std::error::Error>> {
    let mut thread_results = Vec::with_capacity(requests.len());

    std::thread::scope(|scope| {
        let mut handles = Vec::with_capacity(requests.len());
        for thread_requests in requests {
            let index = index.clone();
            handles.push(scope.spawn(move || {
                let mut histogram = Histogram::<u64>::new(3).unwrap();
                let mut total_latency_ns = 0u128;
                let mut count = 0usize;

                for &key_id in thread_requests {
                    let key = ycsb_key_bytes(key_id, key_size);
                    let start = Instant::now();
                    let value = index.get_point(&key);
                    let latency_ns = start.elapsed().as_nanos();
                    debug_assert_eq!(value, payload);
                    histogram.record(latency_ns as u64).unwrap();
                    total_latency_ns += latency_ns;
                    count += 1;
                }

                ThreadResult {
                    count,
                    total_latency_ns,
                    histogram,
                }
            }));
        }

        for handle in handles {
            thread_results.push(handle.join().unwrap());
        }
    });

    Ok(thread_results)
}

impl From<LookupDistribution> for YcsbDistribution {
    fn from(value: LookupDistribution) -> Self {
        match value {
            LookupDistribution::Uniform => YcsbDistribution::Uniform,
            LookupDistribution::Zipf => YcsbDistribution::Zipf,
        }
    }
}

impl std::fmt::Display for LookupDistribution {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LookupDistribution::Uniform => write!(f, "uniform"),
            LookupDistribution::Zipf => write!(f, "zipf"),
        }
    }
}
