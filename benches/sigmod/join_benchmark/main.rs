mod cli;
mod data_source;
mod interface;

use clap::Parser;
use cli::{Cli, TableType};
use data_source::{LineItemDataSource, PartDataSource};
use fbtree::{
    bp::{get_in_mem_pool, ContainerKey},
    mvcc_index::dual_heap_hash::chained_hash_table::ChainedHashTable,
    mvcc_index::hash_heap::hash_heap_table::HeapHashTable,
    mvcc_index::ts_partitioned::ts_partitioned_table::TsPartitionedTable,
    prelude::Timestamp,
};
use interface::{BoxMVIndex, MultiVersionJoinTable};

fn create_table(cli: &Cli, c_key: ContainerKey) -> BoxMVIndex {
    let mem_pool = get_in_mem_pool();
    match cli.table_type {
        TableType::Chain => Box::new(ChainedHashTable::new_with_bucket_num(
            c_key,
            mem_pool,
            cli.num_buckets,
        )) as BoxMVIndex,
        TableType::Heap => Box::new(HeapHashTable::new_with_bucket_num(
            c_key,
            mem_pool,
            cli.num_buckets,
        )) as BoxMVIndex,
        TableType::TsPartitioned => Box::new(TsPartitionedTable::new_with_bucket_num(
            c_key,
            mem_pool,
            cli.num_buckets,
        )) as BoxMVIndex,
        TableType::Naive => Box::new(
            fbtree::naive_hash_index::NaiveMvHashTable::new_with_bucket_num(
                c_key,
                mem_pool,
                cli.num_buckets,
            ),
        ) as BoxMVIndex,
    }
}

fn build_table(
    table: &BoxMVIndex,
    source: &PartDataSource,
    is_naive: bool,
    mark_ts: Timestamp,
) -> std::time::Duration {
    use std::time::Instant;

    println!("=== Building PART Table ===");
    let parts = source.get_parts();
    println!("Inserting {} rows...", parts.len());

    // Prepare all data before timing (for fair comparison)
    let prepared_data: Vec<(Vec<u8>, Vec<u8>, Vec<u8>)> = parts
        .iter()
        .map(|part| {
            (
                part.generate_join_key(),
                part.generate_pkey(),
                part.generate_value(),
            )
        })
        .collect();

    // For naive table type, mark the timestamp after building and measure that time
    if is_naive {
        let mark_duration = table.insert_naive(mark_ts, prepared_data);
        println!("Build complete. Total rows: {}", parts.len());
        // println!("  - Insert time: {:?}", insert_duration);
        println!("  - Insert time: {:?}", mark_duration);
        println!();
        return mark_duration;
    }

    let start = Instant::now();

    for (join_key, pkey, value) in prepared_data {
        table.insert(&join_key, &pkey, &value);
    }

    let insert_duration = start.elapsed();

    println!("Build complete. Total rows: {}", parts.len());
    println!("  - Insert time: {:?}", insert_duration);
    println!();

    insert_duration
}

fn update(
    table: &BoxMVIndex,
    source: &PartDataSource,
    is_naive: bool,
    use_write_repair: bool,
) -> (Timestamp, std::time::Duration) {
    use std::time::Instant;

    println!("=== Phase 3: Apply Updates ===");

    if is_naive {
        // For Naive table type, update is a no-op
        // The updated data will be loaded in Phase 4 (rebuild)
        println!("Naive table type: skipping update (will rebuild table with updated data)");
        println!();
        return (0, std::time::Duration::ZERO);
    }

    let updates = source.get_parts();
    println!("Applying {} updates...", updates.len());

    // Use increasing timestamps for each update
    let base_ts: Timestamp = 1;
    let mut last_ts = base_ts;

    let start = Instant::now();

    for (i, update) in updates.iter().enumerate() {
        let join_key = update.generate_join_key();
        let pkey = update.generate_pkey();
        let value = update.generate_value();

        // Each update gets an increasing timestamp
        let update_ts: Timestamp = base_ts + i as u64;
        last_ts = update_ts;

        if use_write_repair {
            table.update_write_repair(&join_key, &pkey, &value, update_ts);
        } else {
            table.update(&join_key, &pkey, &value, update_ts);
        }
    }

    let update_duration = start.elapsed();

    println!("Updates applied. Total updates: {}", updates.len());
    println!("  - Update time: {:?}", update_duration);
    println!();

    (last_ts, update_duration)
}

fn probe(
    table: &BoxMVIndex,
    source: &LineItemDataSource,
    probe_ts: Timestamp,
    phase_name: &str,
) -> (Vec<Vec<(Vec<u8>, Vec<u8>)>>, std::time::Duration) {
    use std::time::Instant;

    println!("=== {} ===", phase_name);
    let items = source.get_items();
    println!(
        "Probing {} lineitem rows at ts={}...",
        items.len(),
        probe_ts
    );

    let mut results: Vec<Vec<(Vec<u8>, Vec<u8>)>> = Vec::with_capacity(items.len());

    let mut probe_with_match = 0;
    let mut total_matches = 0;

    let start = Instant::now();

    for (i, item) in items.iter().enumerate() {
        let probe_key = item.generate_probe_key();
        let result = table.probe(&probe_key, probe_ts);

        if !result.is_empty() {
            probe_with_match += 1;
            total_matches += result.len();
        }

        if i < 5 || (i >= items.len() - 5) {
            println!(
                "  Probe {}: l_partkey={} -> {} matches",
                i,
                item.l_partkey,
                result.len()
            );
        }

        results.push(result);
    }

    let probe_duration = start.elapsed();

    println!("Probe complete:");
    println!("  - Total probes: {}", items.len());
    println!("  - Probes with matches: {}", probe_with_match);
    println!("  - Total matches: {}", total_matches);
    if probe_with_match > 0 {
        println!(
            "  - Average matches per matching probe: {:.2}",
            total_matches as f64 / probe_with_match as f64
        );
    }
    println!("  - Probe time: {:?}", probe_duration);
    println!();

    (results, probe_duration)
}

fn main() {
    let cli = Cli::parse();
    let is_naive = cli.table_type == TableType::Naive;

    println!("=== JOIN Benchmark ===");
    println!("Table Type: {:?}", cli.table_type);
    println!("Number of Buckets: {}", cli.num_buckets);
    println!("Part File: {}", cli.part_file);
    println!("Part Updates File: {}", cli.part_updates_file);
    println!("Lineitem File: {}", cli.lineitem_file);
    println!();

    // Load data sources
    println!("=== Loading Data Sources ===");
    let part_source = PartDataSource::new(&cli.part_file);
    let updates_source = PartDataSource::new(&cli.part_updates_file);
    let updated_source = PartDataSource::new(&cli.part_updated_file);
    let lineitem_source = LineItemDataSource::new(&cli.lineitem_file);
    println!();

    if is_naive {
        // Naive workflow: create_table -> probe -> update(no-op) -> rebuild -> probe
        let c_key = ContainerKey::new(0, 0);
        let table = create_table(&cli, c_key);
        println!();

        // Phase 1: Build PART table (original data)
        let _build_time_1 = build_table(&table, &part_source, is_naive, 0);

        // Phase 2: Probe before updates (ts=0)
        let (_probe_results_before, _probe_time_1) = probe(
            &table,
            &lineitem_source,
            0,
            "Phase 2: Probe (Before Updates)",
        );

        // Phase 3: Update (no-op for naive)
        let _ = update(&table, &updates_source, is_naive, false);

        // Phase 4: Rebuild table with updated data
        let _build_time_2 = build_table(&table, &updated_source, is_naive, 0);

        // Phase 5: Probe after rebuild
        let (_probe_results_after, _probe_time_2) = probe(
            &table,
            &lineitem_source,
            0,
            "Phase 5: Probe (After Rebuild)",
        );
    } else {
        // Non-naive workflow: run 3 repair types (NoRepair, ReadRepair, WriteRepair)
        // Each repair type does: JOIN(probe) -> UPDATE -> JOIN(probe)

        // No Repair
        println!();
        println!("========================================");
        println!("=== No Repair ===");
        println!("========================================");
        {
            let c_key = ContainerKey::new(0, 0);
            let table = create_table(&cli, c_key);

            // Phase 1: Build PART table
            let _build_time = build_table(&table, &part_source, is_naive, 0);

            // Phase 2: Probe before updates (ts=0)
            let (_probe_results_before, _probe_time_1) = probe(
                &table,
                &lineitem_source,
                0,
                "Phase 2: Probe (Before Updates)",
            );

            // Phase 3: Apply updates (no repair)
            let (last_update_ts, _update_duration) =
                update(&table, &updates_source, is_naive, false);

            // Phase 4: Probe after updates
            let probe_ts = last_update_ts + 1;
            let (_probe_results_after, _probe_time_2) = probe(
                &table,
                &lineitem_source,
                probe_ts,
                "Phase 4: Probe (After Updates)",
            );
        }

        // Read Repair
        println!();
        println!("========================================");
        println!("=== Read Repair ===");
        println!("========================================");
        {
            let c_key = ContainerKey::new(0, 1);
            let table = create_table(&cli, c_key);

            // Phase 1: Build PART table
            let _build_time = build_table(&table, &part_source, is_naive, 0);

            // Phase 2: Probe before updates (ts=0) - same as no repair
            let (_probe_results_before, _probe_time_1) = probe(
                &table,
                &lineitem_source,
                0,
                "Phase 2: Probe (Before Updates)",
            );

            // Phase 3: Apply updates (no repair for read repair - same as no repair)
            let (last_update_ts, _update_duration) =
                update(&table, &updates_source, is_naive, false);

            // Phase 4: Probe after updates - same as no repair
            let probe_ts = last_update_ts + 1;
            let (_probe_results_after, _probe_time_2) = probe(
                &table,
                &lineitem_source,
                probe_ts,
                "Phase 4: Probe (After Updates)",
            );
        }

        // Write Repair
        println!();
        println!("========================================");
        println!("=== Write Repair ===");
        println!("========================================");
        {
            let c_key = ContainerKey::new(0, 2);
            let table = create_table(&cli, c_key);

            // Phase 1: Build PART table
            let _build_time = build_table(&table, &part_source, is_naive, 0);

            // Phase 2: Probe before updates (ts=0)
            let (_probe_results_before, _probe_time_1) = probe(
                &table,
                &lineitem_source,
                0,
                "Phase 2: Probe (Before Updates)",
            );

            // Phase 3: Apply updates (with write repair)
            let (last_update_ts, _update_duration) =
                update(&table, &updates_source, is_naive, true);

            // Phase 4: Probe after updates
            let probe_ts = last_update_ts + 1;
            let (_probe_results_after, _probe_time_2) = probe(
                &table,
                &lineitem_source,
                probe_ts,
                "Phase 4: Probe (After Updates)",
            );
        }
    }

    println!();
    println!("=== Benchmark Complete ===");
}
