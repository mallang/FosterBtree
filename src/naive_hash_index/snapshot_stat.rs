#[derive(Debug, Clone, Default)]
pub struct SnapshotStat {
    pub readable_timestamps_published: usize,
    pub retained_snapshots: usize,
    pub snapshots_built_total: usize,
    pub snapshot_reads_total: usize,
    pub snapshot_cache_hits_total: usize,
    pub snapshot_cache_misses_total: usize,
    pub current_reads_total: usize,
}
