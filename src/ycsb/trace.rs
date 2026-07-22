use std::{
    fs::File,
    io::{self, BufReader, BufWriter, Read, Write},
    path::Path,
};

use rand::{rngs::SmallRng, RngCore, SeedableRng};

use crate::random::FastZipf;

use super::txn_utils::get_key_bytes;

const MAGIC: &[u8; 8] = b"HVTYCSB1";
const VERSION: u64 = 1;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum YcsbDistribution {
    Uniform,
    Zipf,
}

impl YcsbDistribution {
    pub fn as_str(self) -> &'static str {
        match self {
            YcsbDistribution::Uniform => "uniform",
            YcsbDistribution::Zipf => "zipf",
        }
    }

    fn to_u8(self) -> u8 {
        match self {
            YcsbDistribution::Uniform => 0,
            YcsbDistribution::Zipf => 1,
        }
    }

    fn from_u8(value: u8) -> io::Result<Self> {
        match value {
            0 => Ok(YcsbDistribution::Uniform),
            1 => Ok(YcsbDistribution::Zipf),
            other => Err(invalid_data(format!(
                "unknown YCSB lookup distribution id: {other}"
            ))),
        }
    }
}

#[derive(Clone, Debug)]
pub struct YcsbLookupTraceConfig {
    pub n_keys: usize,
    pub key_size: usize,
    pub lookups_per_thread: usize,
    pub warmup_lookups_per_thread: usize,
    pub threads: usize,
    pub distribution: YcsbDistribution,
    pub zipf_theta: f64,
    pub seed: u64,
}

impl YcsbLookupTraceConfig {
    pub fn validate(&self) -> io::Result<()> {
        if self.n_keys == 0 {
            return Err(invalid_input("n_keys must be greater than zero"));
        }
        if self.key_size < std::mem::size_of::<usize>() {
            return Err(invalid_input(format!(
                "key_size must be at least {} bytes",
                std::mem::size_of::<usize>()
            )));
        }
        if self.threads == 0 {
            return Err(invalid_input("threads must be greater than zero"));
        }
        if self.lookups_per_thread == 0 {
            return Err(invalid_input(
                "lookups_per_thread must be greater than zero",
            ));
        }
        if self.distribution == YcsbDistribution::Zipf && !(0.0..1.0).contains(&self.zipf_theta) {
            return Err(invalid_input(
                "zipf_theta must be in [0.0, 1.0) for the current FastZipf generator",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub struct YcsbLookupTrace {
    pub config: YcsbLookupTraceConfig,
    pub warmup: Vec<Vec<usize>>,
    pub measured: Vec<Vec<usize>>,
}

pub fn ycsb_key_bytes(key_id: usize, key_size: usize) -> Vec<u8> {
    get_key_bytes(key_id, key_size)
}

pub fn make_lookup_trace(config: YcsbLookupTraceConfig) -> io::Result<YcsbLookupTrace> {
    config.validate()?;
    let warmup = make_thread_requests(
        config.n_keys,
        config.warmup_lookups_per_thread,
        config.threads,
        config.distribution,
        config.zipf_theta,
        config.seed ^ 0x9e37_79b9_7f4a_7c15,
    );
    let measured = make_thread_requests(
        config.n_keys,
        config.lookups_per_thread,
        config.threads,
        config.distribution,
        config.zipf_theta,
        config.seed,
    );
    Ok(YcsbLookupTrace {
        config,
        warmup,
        measured,
    })
}

pub fn write_lookup_trace(path: impl AsRef<Path>, trace: &YcsbLookupTrace) -> io::Result<()> {
    trace.config.validate()?;
    validate_trace_shape(trace)?;

    if let Some(parent) = path.as_ref().parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent)?;
        }
    }

    let mut writer = BufWriter::new(File::create(path)?);
    writer.write_all(MAGIC)?;
    write_u64(&mut writer, VERSION)?;
    write_u64(&mut writer, trace.config.n_keys as u64)?;
    write_u64(&mut writer, trace.config.key_size as u64)?;
    write_u64(&mut writer, trace.config.threads as u64)?;
    write_u64(&mut writer, trace.config.warmup_lookups_per_thread as u64)?;
    write_u64(&mut writer, trace.config.lookups_per_thread as u64)?;
    writer.write_all(&[trace.config.distribution.to_u8()])?;
    writer.write_all(&[0_u8; 7])?;
    writer.write_all(&trace.config.zipf_theta.to_le_bytes())?;
    write_u64(&mut writer, trace.config.seed)?;

    write_request_section(&mut writer, &trace.warmup)?;
    write_request_section(&mut writer, &trace.measured)?;
    writer.flush()
}

pub fn read_lookup_trace(path: impl AsRef<Path>) -> io::Result<YcsbLookupTrace> {
    let mut reader = BufReader::new(File::open(path)?);

    let mut magic = [0_u8; 8];
    reader.read_exact(&mut magic)?;
    if &magic != MAGIC {
        return Err(invalid_data("not a hash-vs-tree YCSB lookup trace file"));
    }

    let version = read_u64(&mut reader)?;
    if version != VERSION {
        return Err(invalid_data(format!(
            "unsupported YCSB lookup trace version: {version}"
        )));
    }

    let n_keys = usize_from_u64(read_u64(&mut reader)?, "n_keys")?;
    let key_size = usize_from_u64(read_u64(&mut reader)?, "key_size")?;
    let threads = usize_from_u64(read_u64(&mut reader)?, "threads")?;
    let warmup_lookups_per_thread =
        usize_from_u64(read_u64(&mut reader)?, "warmup_lookups_per_thread")?;
    let lookups_per_thread = usize_from_u64(read_u64(&mut reader)?, "lookups_per_thread")?;

    let mut distribution = [0_u8; 1];
    reader.read_exact(&mut distribution)?;
    let distribution = YcsbDistribution::from_u8(distribution[0])?;

    let mut padding = [0_u8; 7];
    reader.read_exact(&mut padding)?;

    let mut theta_bytes = [0_u8; 8];
    reader.read_exact(&mut theta_bytes)?;
    let zipf_theta = f64::from_le_bytes(theta_bytes);
    let seed = read_u64(&mut reader)?;

    let config = YcsbLookupTraceConfig {
        n_keys,
        key_size,
        lookups_per_thread,
        warmup_lookups_per_thread,
        threads,
        distribution,
        zipf_theta,
        seed,
    };
    config.validate()?;

    let warmup = read_request_section(&mut reader, threads, warmup_lookups_per_thread)?;
    let measured = read_request_section(&mut reader, threads, lookups_per_thread)?;
    let trace = YcsbLookupTrace {
        config,
        warmup,
        measured,
    };
    validate_trace_shape(&trace)?;
    Ok(trace)
}

fn make_thread_requests(
    n_keys: usize,
    requests_per_thread: usize,
    threads: usize,
    distribution: YcsbDistribution,
    zipf_theta: f64,
    seed: u64,
) -> Vec<Vec<usize>> {
    (0..threads)
        .map(|thread_id| {
            make_requests(
                n_keys,
                requests_per_thread,
                distribution,
                zipf_theta,
                seed.wrapping_add(thread_id as u64),
            )
        })
        .collect()
}

fn make_requests(
    n_keys: usize,
    count: usize,
    distribution: YcsbDistribution,
    zipf_theta: f64,
    seed: u64,
) -> Vec<usize> {
    let mut requests = Vec::with_capacity(count);
    match distribution {
        YcsbDistribution::Uniform => {
            let mut rng = SmallRng::seed_from_u64(seed);
            for _ in 0..count {
                requests.push((rng.next_u64() as usize) % n_keys);
            }
        }
        YcsbDistribution::Zipf => {
            let mut zipf = FastZipf::new(SmallRng::seed_from_u64(seed), zipf_theta, n_keys);
            for _ in 0..count {
                requests.push(zipf.sample() % n_keys);
            }
        }
    }
    requests
}

fn validate_trace_shape(trace: &YcsbLookupTrace) -> io::Result<()> {
    validate_request_section(
        "warmup",
        &trace.warmup,
        trace.config.threads,
        trace.config.warmup_lookups_per_thread,
        trace.config.n_keys,
    )?;
    validate_request_section(
        "measured",
        &trace.measured,
        trace.config.threads,
        trace.config.lookups_per_thread,
        trace.config.n_keys,
    )
}

fn validate_request_section(
    name: &str,
    requests: &[Vec<usize>],
    threads: usize,
    requests_per_thread: usize,
    n_keys: usize,
) -> io::Result<()> {
    if requests.len() != threads {
        return Err(invalid_data(format!(
            "{name} trace has {} threads, expected {threads}",
            requests.len()
        )));
    }
    for (thread_id, thread_requests) in requests.iter().enumerate() {
        if thread_requests.len() != requests_per_thread {
            return Err(invalid_data(format!(
                "{name} trace thread {thread_id} has {} requests, expected {requests_per_thread}",
                thread_requests.len()
            )));
        }
        if let Some(&bad_key) = thread_requests.iter().find(|&&key| key >= n_keys) {
            return Err(invalid_data(format!(
                "{name} trace thread {thread_id} contains key {bad_key}, but n_keys is {n_keys}"
            )));
        }
    }
    Ok(())
}

fn write_request_section(writer: &mut impl Write, requests: &[Vec<usize>]) -> io::Result<()> {
    for thread_requests in requests {
        for &key_id in thread_requests {
            write_u64(writer, key_id as u64)?;
        }
    }
    Ok(())
}

fn read_request_section(
    reader: &mut impl Read,
    threads: usize,
    requests_per_thread: usize,
) -> io::Result<Vec<Vec<usize>>> {
    let mut requests = Vec::with_capacity(threads);
    for _ in 0..threads {
        let mut thread_requests = Vec::with_capacity(requests_per_thread);
        for _ in 0..requests_per_thread {
            thread_requests.push(usize_from_u64(read_u64(reader)?, "key_id")?);
        }
        requests.push(thread_requests);
    }
    Ok(requests)
}

fn write_u64(writer: &mut impl Write, value: u64) -> io::Result<()> {
    writer.write_all(&value.to_le_bytes())
}

fn read_u64(reader: &mut impl Read) -> io::Result<u64> {
    let mut bytes = [0_u8; 8];
    reader.read_exact(&mut bytes)?;
    Ok(u64::from_le_bytes(bytes))
}

fn usize_from_u64(value: u64, field: &str) -> io::Result<usize> {
    usize::try_from(value).map_err(|_| invalid_data(format!("{field} is too large: {value}")))
}

fn invalid_input(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message.into())
}

fn invalid_data(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.into())
}
