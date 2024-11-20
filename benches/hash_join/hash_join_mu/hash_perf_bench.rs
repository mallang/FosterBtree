use fbtree::mvcc_index::MvccIndex;
use fbtree::{mvcc_index::hash_join::mvcc_hash_join::MvccHashJoinTable, prelude::*};
// use fbtree::{mvcc_index::hashtable_mu::mvcc_hash_join_cuckoo::HashJoinTable, prelude::*};
use serde::de::value;

use std::collections::HashMap;
use std::error::Error;
use std::sync::Arc;
use std::time::Instant;

fn bench_update_perflog() -> Result<(), Box<dyn Error>> {
    // Initialize the hash join table using the MvccIndex trait
    let mem_pool = get_in_mem_pool(); // You need to implement or import this function
    let c_key = ContainerKey::new(0, 0);
    let hash_join_table = MvccHashJoinTable::create(c_key, mem_pool.clone())?;

    let data_num = 10000 as usize;
    let data = (0..data_num)
        .into_iter()
        .map(|i| {
            (
                format!("key_{:06}", i).as_bytes().to_vec(),
                format!("pkey_{:06}", i).as_bytes().to_vec(),
                format!("value_{:06}", i).as_bytes().to_vec(),
            )
        })
        .collect::<Vec<_>>();
    let value_new = (0..data_num)
        .into_iter()
        .map(|i| format!("new_value_{:06}", i).as_bytes().to_vec())
        .collect::<Vec<_>>();

    // BENCH HASH_JOIN_TABLE UPDATE

    // Load data into the hash join table
    for (key, pkey, value) in &data {
        hash_join_table.insert(key.clone(), pkey.clone(), 0, 0, value.clone())?;
    }

    {
        let data_clone = data.clone();
        let value_new_clone = value_new.clone();

        //
        // Measure and report data UPDATING time for HashJoinTable
        let start_time_hj_load = Instant::now();
        for ((key, pkey, _value), new_value) in
            (data_clone).into_iter().zip((value_new_clone).into_iter())
        {
            // UPDATE data
            hash_join_table.update(key, pkey, 1, 0, new_value).unwrap();
        }
        let duration_hj_load = start_time_hj_load.elapsed();
        println!(
            "UPDATED {} entries into HashJoinTable in {:.2?}",
            data_num, duration_hj_load
        );
    }

    // for perf.log -- add update coverage time
    {
        let data_clone = data.clone();
        let value_new_clone = value_new
            .clone()
            .into_iter()
            .map(|mut ve| {
                ve.push(233);
                ve
            })
            .collect::<Vec<_>>();

        //
        // Measure and report data UPDATING time for HashJoinTable
        let start_time_hj_load = Instant::now();
        for ((key, pkey, _value), new_value) in
            (data_clone).into_iter().zip((value_new_clone).into_iter())
        {
            // UPDATE data
            hash_join_table.update(key, pkey, 1, 0, new_value).unwrap();
        }
        let duration_hj_load = start_time_hj_load.elapsed();
        println!(
            "UPDATED {} entries into HashJoinTable in {:.2?}",
            data_num, duration_hj_load
        );
    }

    Ok(())
}

fn bench_update() -> Result<(), Box<dyn Error>> {
    // Initialize the hash join table using the MvccIndex trait
    let mem_pool = get_in_mem_pool(); // You need to implement or import this function
    let c_key = ContainerKey::new(0, 0);
    let hash_join_table = MvccHashJoinTable::create(c_key, mem_pool.clone())?;

    let data_num = 10000 as usize;
    let data = (0..data_num)
        .into_iter()
        .map(|i| {
            (
                format!("key_{:06}", i).as_bytes().to_vec(),
                format!("pkey_{:06}", i).as_bytes().to_vec(),
                format!("value_{:06}", i).as_bytes().to_vec(),
            )
        })
        .collect::<Vec<_>>();
    let value_new = (0..data_num)
        .into_iter()
        .map(|i| format!("new_value_{:06}", i).as_bytes().to_vec())
        .collect::<Vec<_>>();

    {
        // Initialize Rust's default HashMap
        let mut rust_hash_map: HashMap<(Vec<u8>, Vec<u8>), Vec<u8>> = HashMap::new();

        // //
        // // Measure and report data loading time for Rust HashMap
        // let start_time_hashmap_load = Instant::now();

        // Load data into Rust's HashMap
        for (key, pkey, value) in &data {
            rust_hash_map.insert((key.clone(), pkey.clone()), value.clone());
        }

        let start_time_hashmap_load = Instant::now();
        // UPDATE data
        for ((key, pkey, value), new_value) in (&data).iter().zip((&value_new).iter()) {
            rust_hash_map.insert((key.clone(), pkey.clone()), new_value.clone());
        }
        let duration_hashmap_load = start_time_hashmap_load.elapsed();
        println!(
            "UPDATE {} entries into Rust HashMap in {:.2?}",
            data_num, duration_hashmap_load
        );

        for ((key, pkey, _value), new_value) in (&data).iter().zip((&value_new).iter()) {
            let a = rust_hash_map.get(&(key.clone(), pkey.clone())).unwrap();
            // assert_eq!(a.as_ref().unwrap(), new_value);
            assert_eq!(a, new_value);
        }
    }

    // BENCH HASH_JOIN_TABLE UPDATE

    // Load data into the hash join table
    for (key, pkey, value) in &data {
        hash_join_table.insert(key.clone(), pkey.clone(), 0, 0, value.clone())?;
    }

    {
        let data_clone = data.clone();
        let value_new_clone = value_new.clone();

        //
        // Measure and report data UPDATING time for HashJoinTable
        let start_time_hj_load: Instant = Instant::now();
        for ((key, pkey, _value), new_value) in
            (data_clone).into_iter().zip((value_new_clone).into_iter())
        {
            // UPDATE data
            hash_join_table.update(key, pkey, 1, 0, new_value).unwrap();
        }
        let duration_hj_load = start_time_hj_load.elapsed();
        println!(
            "UPDATED {} entries into HashJoinTable in {:.2?}",
            data_num, duration_hj_load
        );
    }

    for ((key, pkey, _value), new_value) in (&data).iter().zip((&value_new).iter()) {
        let a = hash_join_table.get(key, pkey, 1)?;
        // assert_eq!(a.as_ref().unwrap(), new_value);
        assert_eq!(&a, new_value);
    }

    Ok(())
}

fn bench_insert() -> Result<(), Box<dyn Error>> {
    // Initialize the hash join table using the MvccIndex trait
    let mem_pool = get_in_mem_pool(); // You need to implement or import this function
    let c_key = ContainerKey::new(0, 0);
    let hash_join_table = MvccHashJoinTable::create(c_key, mem_pool.clone())?;

    let data_num = 10000 as usize;
    let data = (0..data_num)
        .into_iter()
        .map(|i| {
            (
                format!("key_{:06}", i).as_bytes().to_vec(),
                format!("pkey_{:06}", i).as_bytes().to_vec(),
                format!("value_{:06}", i).as_bytes().to_vec(),
            )
        })
        .collect::<Vec<_>>();

    {
        // Initialize Rust's default HashMap
        let mut rust_hash_map: HashMap<(Vec<u8>, Vec<u8>), Vec<u8>> = HashMap::new();

        // //
        // // Measure and report data loading time for Rust HashMap
        // let start_time_hashmap_load = Instant::now();

        // Load data into Rust's HashMap
        for (key, pkey, value) in &data {
            rust_hash_map.insert((key.clone(), pkey.clone()), value.clone());
        }

        let data_clone = data.clone();

        let start_time_hashmap_load = Instant::now();

        for (key, pkey, _value) in data_clone {
            let a = rust_hash_map.get(&(key, pkey)).unwrap();
            // assert_eq!(a.as_ref().unwrap(), _value);
            assert_eq!(a, &_value);
        }

        let duration_hashmap_load = start_time_hashmap_load.elapsed();
        println!(
            "Loaded {} entries into Rust HashMap in {:.2?}",
            data_num, duration_hashmap_load
        );
    }

    // //
    // // Measure and report data loading time for HashJoinTable
    // let start_time_hj_load = Instant::now();

    // Load data into the hash join table
    for (key, pkey, value) in &data {
        hash_join_table.insert(key.clone(), pkey.clone(), 0, 0, value.clone())?;
    }

    //
    // Measure and report data loading time for HashJoinTable
    let start_time_hj_load = Instant::now();

    for (key, pkey, _value) in &data {
        let a = hash_join_table.get(&key, &pkey, 0)?;
        //  assert_eq!(a.as_ref().unwrap(), _value);
        assert_eq!(&a, _value);
    }

    let duration_hj_load = start_time_hj_load.elapsed();
    println!(
        "Loaded {} entries into HashJoinTable in {:.2?}",
        data_num, duration_hj_load
    );

    Ok(())
}

fn main() -> Result<(), Box<dyn Error>> {
    bench_insert()?;
    bench_update()?;
    Ok(())
}
