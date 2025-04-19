#[cfg(test)]
mod test_ops {
    use dashmap::mapref::entry;

    use crate::bp::{get_in_mem_pool, ContainerKey, InMemPool};

    use crate::log_warn;
    use crate::mvcc_index::hash_heap::hash_heap_table::{self, HeapHashTable};
    use crate::mvcc_index::hash_join::chained_hash_table::ChainedHashTable;
    use crate::mvcc_index::linear_hash::linear_hash_table::linear_hash_table::LinearHashTable;
    use crate::mvcc_index::ts_partitioned::ts_partitioned_table::TsPartitionedTable;
    use crate::mvcc_index::{hash_join, Delta, MvccIndex};
    use crate::page::{Page, PageId, AVAILABLE_PAGE_SIZE};
    use crate::prelude::Timestamp;
    use core::str;
    use std::marker::PhantomData;
    use std::sync::Arc;

    #[test]
    fn test_hash_table_scan_key() {
        test_scan_key_ops::<LinearHashTable<_>>();
        test_scan_key_ops::<ChainedHashTable<_>>();
        test_scan_key_ops::<HeapHashTable<_>>();
        test_scan_key_ops::<TsPartitionedTable<_>>();
    }

    #[test]
    fn test_hash_table_basic_ops() {
        test_basic_index_ops::<LinearHashTable<_>>();
        test_basic_index_ops::<ChainedHashTable<_>>();
        test_basic_index_ops::<TsPartitionedTable<_>>();
        test_basic_index_ops::<HeapHashTable<_>>();
    }

    #[test]
    fn test_delta_scan() {
        test_delta_scan_op0::<LinearHashTable<_>>();
        test_delta_scan_op0::<ChainedHashTable<_>>();
        test_delta_scan_op0::<HeapHashTable<_>>();
        test_delta_scan_op0::<TsPartitionedTable<_>>();

        test_delta_scan_op1::<TsPartitionedTable<_>>();
        test_delta_scan_op1::<ChainedHashTable<_>>();
        test_delta_scan_op1::<HeapHashTable<_>>();
        test_delta_scan_op1::<TsPartitionedTable<_>>();
    }

    #[test]
    fn test_hash_table_garbage_collection() {
        test_garbage_collection::<LinearHashTable<_>>();
        test_garbage_collection::<ChainedHashTable<_>>();
        test_garbage_collection::<HeapHashTable<_>>();
        test_garbage_collection::<TsPartitionedTable<_>>();
    }

    #[test]
    fn test_read_repair() {
        test_read_repair_ops::<LinearHashTable<_>>();
        test_read_repair_ops::<ChainedHashTable<_>>();
        test_read_repair_ops::<TsPartitionedTable<_>>();
        test_read_repair_ops::<HeapHashTable<_>>();
    }

    fn test_read_repair_ops<I>()
    where
        I: MvccIndex<InMemPool, Key = Vec<u8>, PKey = Vec<u8>, Value = Vec<u8>>,
    {
        test_get_read_repair::<I>();
        test_scan_read_repair::<I>();
        test_scan_delta_read_repair::<I>();
        test_scan_key_vec_read_repair::<I>();
    }

    fn test_get_read_repair<I>()
    where
        I: MvccIndex<InMemPool, Key = Vec<u8>, PKey = Vec<u8>, Value = Vec<u8>>,
    {
        let num = 100;
        let mem_pool = get_in_mem_pool();
        let c_key = ContainerKey::new(0, 0);
        let hash_join_table = Arc::new(I::create_with_bucket_num(c_key, mem_pool, 16).unwrap());

        // 0..num inserts
        for i in (0..num).into_iter().step_by(1) {
            let key = format!("key{}", i).into_bytes();
            let pkey = format!("pkey{}", i).into_bytes();
            let value = format!("value{}", i).into_bytes();
            hash_join_table.insert(key, pkey, 1, 1, value).unwrap();
        }

        hash_join_table.split_at_ts(2).unwrap();

        // 0..num updates
        for i in (0..num).into_iter().step_by(1) {
            let key = format!("key{}", i).into_bytes();
            let pkey = format!("pkey{}", i).into_bytes();
            let value = format!("value{}", i + 10000).into_bytes();
            hash_join_table.update(key, pkey, 3, 1, value).unwrap();
        }

        hash_join_table.split_at_ts(4).unwrap();

        // 0..num updates
        for i in (0..num).into_iter().step_by(1) {
            let key = format!("key{}", i).into_bytes();
            let pkey = format!("pkey{}", i).into_bytes();
            let value = format!("value{}", i + 20000).into_bytes();
            hash_join_table.update(key, pkey, 4, 1, value).unwrap();
        }

        // 0..num updates
        for i in (0..num).into_iter().step_by(1) {
            let key = format!("key{}", i).into_bytes();
            let pkey = format!("pkey{}", i).into_bytes();
            let value = format!("value{}", i + 30000).into_bytes();
            hash_join_table.update(key, pkey, 5, 1, value).unwrap();
        }

        for i in (0..num).into_iter().step_by(1) {
            let key = format!("key{}", i).into_bytes();
            let pkey = format!("pkey{}", i).into_bytes();
            // let value = format!("value{}", i + 20000).into_bytes();
            let a = hash_join_table.get_read_repair(&key, &pkey, 6).unwrap();
            assert_eq!(a, Some(format!("value{}", i + 30000).as_bytes().to_vec()));
        }

        for entry in hash_join_table.scan_all().unwrap() {
            if entry.start_ts() == 4 {
                assert_eq!(entry.end_ts(), 5, "entry: {:?}", entry);
            }
        }
    }

    fn test_scan_delta_read_repair<I>()
    where
        I: MvccIndex<InMemPool, Key = Vec<u8>, PKey = Vec<u8>, Value = Vec<u8>>,
    {
        let num = 100;
        let mem_pool = get_in_mem_pool();
        let c_key = ContainerKey::new(0, 0);
        let hash_join_table = Arc::new(I::create_with_bucket_num(c_key, mem_pool, 16).unwrap());

        // 0..num inserts
        for i in (0..num).into_iter().step_by(1) {
            let key = format!("key{}", i).into_bytes();
            let pkey = format!("pkey{}", i).into_bytes();
            let value = format!("value{}", i).into_bytes();
            hash_join_table.insert(key, pkey, 1, 1, value).unwrap();
        }

        hash_join_table.split_at_ts(2).unwrap();

        // 0..num updates
        for i in (0..num - 1).into_iter().step_by(1) {
            let key = format!("key{}", i).into_bytes();
            let pkey = format!("pkey{}", i).into_bytes();
            let value = format!("value{}", i + 10000).into_bytes();
            hash_join_table.update(key, pkey, 3, 1, value).unwrap();
        }

        hash_join_table.split_at_ts(4).unwrap();

        {
            let i = num;
            let key = format!("key{}", i).into_bytes();
            let pkey = format!("pkey{}", i).into_bytes();
            let value = format!("value{}", i).into_bytes();
            hash_join_table.insert(key, pkey, 6, 1, value).unwrap();
        }

        for entry in hash_join_table.delta_scan_read_repair(1, 6).unwrap() {
            if &entry.0 == format!("key{}", num - 1).as_bytes() {
                // no delta
                assert!(false, "should not exist in delta");
            } else if &entry.0 == format!("key{}", num).as_bytes() {
                assert_eq!(
                    &entry.2,
                    &Delta::Inserted(format!("value{}", num).as_bytes().to_vec())
                );
            } else {
                let i_str = &entry.0[3..];
                let i = std::str::from_utf8(i_str)
                    .unwrap()
                    .parse::<usize>()
                    .unwrap();
                assert_eq!(
                    &entry.2,
                    &Delta::Updated(format!("value{}", i + 10000).as_bytes().to_vec())
                );
            }
        }

        for entry in hash_join_table.scan_all().unwrap() {
            if entry.start_ts() == 1 || entry.start_ts() == 6 {
                if entry.key() == format!("key{}", num - 1).as_bytes() {
                    assert_eq!(entry.end_ts(), Timestamp::MAX);
                } else if entry.key() == format!("key{}", num).as_bytes() {
                    assert_eq!(entry.end_ts(), Timestamp::MAX);
                } else {
                    assert_eq!(entry.end_ts(), 3, "entry: {:?}", entry);
                }
            }
        }
    }

    fn test_scan_key_vec_read_repair<I>()
    where
        I: MvccIndex<InMemPool, Key = Vec<u8>, PKey = Vec<u8>, Value = Vec<u8>>,
    {
        let mem_pool = get_in_mem_pool();
        let c_key = ContainerKey::new(0, 0);
        let hash_join_table = Arc::new(I::create_with_bucket_num(c_key, mem_pool, 16).unwrap());

        // 1..100 inserts
        for i in (0..100).into_iter().step_by(1) {
            let key = format!("key{}", i).into_bytes();
            let pkey1 = format!("pkey{}", i + 1000).into_bytes();
            let value1 = format!("value{}", i + 1000).into_bytes();
            hash_join_table
                .insert(key.clone(), pkey1.clone(), 1, 1, value1)
                .unwrap();

            let pkey2 = format!("pkey{}", i + 2000).into_bytes();
            let value2 = format!("value{}", i + 2000).into_bytes();
            hash_join_table
                .insert(key.clone(), pkey2.clone(), 1, 1, value2)
                .unwrap();

            let new_value1 = format!("new_value{}", i + 1000).into_bytes();
            hash_join_table
                .update(key.clone(), pkey1.clone(), 20, 1, new_value1)
                .unwrap();
            let new_value2 = format!("new_value{}", i + 2000).into_bytes();
            hash_join_table
                .update(key.clone(), pkey2.clone(), 20, 1, new_value2)
                .unwrap();
        }

        hash_join_table.split_at_ts(21).unwrap();

        // 1..100 inserts
        for i in (0..100).into_iter().step_by(1) {
            let key = format!("key{}", i).into_bytes();
            let pkey1 = format!("pkey{}", i + 1000).into_bytes();
            let pkey2 = format!("pkey{}", i + 2000).into_bytes();

            let new_value1 = format!("new_new_value{}", i + 1000).into_bytes();
            hash_join_table
                .update(key.clone(), pkey1.clone(), 30, 1, new_value1)
                .unwrap();
            let new_value2 = format!("new_new_value{}", i + 2000).into_bytes();
            hash_join_table
                .update(key.clone(), pkey2.clone(), 30, 1, new_value2)
                .unwrap();
        }

        for i in (0..100).into_iter() {
            let key = format!("key{}", i).into_bytes();
            let t = hash_join_table.scan_key_vec_read_repair(&key, 30).unwrap();

            assert!(t
                .iter()
                .any(|x| x.0 == format!("pkey{}", i + 1000).into_bytes()));
            assert!(t
                .iter()
                .any(|x| x.0 == format!("pkey{}", i + 2000).into_bytes()));
        }

        for e in hash_join_table.scan_all().unwrap() {
            if e.start_ts() == 10 {
                assert_eq!(e.end_ts(), 20);
            } else if e.start_ts() == 20 {
                assert_eq!(e.end_ts(), 30);
            } else if e.start_ts() == 30 {
                assert_eq!(e.end_ts(), Timestamp::MAX);
            }
        }
    }

    fn test_scan_read_repair<I>()
    where
        I: MvccIndex<InMemPool, Key = Vec<u8>, PKey = Vec<u8>, Value = Vec<u8>>,
    {
        let mem_pool = get_in_mem_pool();
        let c_key = ContainerKey::new(0, 0);
        let hash_join_table = I::create_with_bucket_num(c_key, mem_pool, 16).unwrap();

        hash_join_table
            .insert(vec![1], vec![1], 1, 1, vec![1])
            .unwrap();
        hash_join_table
            .insert(vec![2], vec![2], 2, 1, vec![2])
            .unwrap();
        hash_join_table
            .update(vec![1], vec![1], 3, 1, vec![3])
            .unwrap();

        hash_join_table.split_at_ts(4).unwrap();

        hash_join_table
            .update(vec![2], vec![2], 4, 1, vec![4])
            .unwrap();
        hash_join_table
            .insert(vec![3], vec![3], 5, 1, vec![5])
            .unwrap();
        hash_join_table
            .update(vec![3], vec![3], 8, 1, vec![8])
            .unwrap();
        let scan_iter = hash_join_table.scan_read_repair(10).unwrap();
        for entry in scan_iter {
            let key = entry.0;
            let value = entry.2;
            if key == &[1] {
                assert_eq!(value, vec![3]);
            } else if key == &[2] {
                assert_eq!(value, vec![4]);
            } else if key == &[3] {
                assert_eq!(value, vec![8]);
            } else {
                assert!(false, "should not reach here, key: {:?}", key);
            }
        }

        // after repair
        let scan_iter = hash_join_table.scan_all().unwrap();
        for entry in scan_iter {
            if entry.key() == &[1] && entry.start_ts() == 1 {
                assert_eq!(entry.end_ts(), 3);
            } else if entry.key() == &[2] && entry.start_ts() == 2 {
                assert_eq!(entry.end_ts(), 4);
            } else if entry.key() == &[3] && entry.start_ts() == 5 {
                assert_eq!(entry.end_ts(), 8);
            }
        }
    }

    /*
        (key1, 1), (key2, 2), (key1, 3), (key2, 4), (key3, 5) |gc at 6| (key3, 8)
        Before GC:
            (key1, 1), (key1, 3),
            (key2, 2), (key2, 4),
            (key3, 5), (key3, 8)

        After GC:
            (key1, 3),
            (key2, 4),
            (key3, 5), (key3, 8),
    */
    fn test_garbage_collection<I>()
    where
        I: MvccIndex<InMemPool, Key = Vec<u8>, PKey = Vec<u8>, Value = Vec<u8>>,
    {
        let mem_pool = get_in_mem_pool();
        let c_key = ContainerKey::new(0, 0);
        let hash_join_table = I::create(c_key, mem_pool).unwrap();
        hash_join_table
            .insert(vec![1], vec![1], 1, 1, vec![1])
            .unwrap();
        hash_join_table
            .insert(vec![2], vec![2], 2, 1, vec![2])
            .unwrap();
        hash_join_table
            .update(vec![1], vec![1], 3, 1, vec![3])
            .unwrap();
        hash_join_table
            .update(vec![2], vec![2], 4, 1, vec![4])
            .unwrap();
        hash_join_table
            .insert(vec![3], vec![3], 5, 1, vec![5])
            .unwrap();
        hash_join_table
            .update(vec![3], vec![3], 8, 1, vec![8])
            .unwrap();
        let scan_iter = hash_join_table.scan(1).unwrap();
        assert_eq!(1, scan_iter.count());
        let scan_iter = hash_join_table.scan(2).unwrap();
        assert_eq!(2, scan_iter.count());
        let scan_iter = hash_join_table.scan(3).unwrap();
        assert_eq!(2, scan_iter.count());

        hash_join_table.garbage_collect(6).unwrap();
        let scan_iter = hash_join_table.scan(100).unwrap();
        for entry in scan_iter {
            let key = entry.0;
            let value = entry.2;
            if &key[..] == b"1" {
                assert_eq!(value, vec![3]);
            } else if &key[..] == b"2" {
                assert_eq!(value, vec![4]);
            } else if &key[..] == b"3" {
                assert_eq!(value, vec![8]);
            }
        }
        let scan_iter = hash_join_table.scan(6).unwrap();
        for entry in scan_iter {
            let key = entry.0;
            let value = entry.2;
            if &key[..] == b"1" {
                assert_eq!(value, vec![3]);
            } else if &key[..] == b"2" {
                assert_eq!(value, vec![4]);
            } else if &key[..] == b"3" {
                assert_eq!(value, vec![5]);
            }
        }

        let scan_iter = hash_join_table.scan(1).unwrap();
        assert_eq!(0, scan_iter.count());
        let scan_iter = hash_join_table.scan(2).unwrap();
        assert_eq!(0, scan_iter.count());
        let scan_iter = hash_join_table.scan(3).unwrap();
        assert_eq!(1, scan_iter.count());
    }

    fn test_basic_index_ops0<I>()
    where
        I: MvccIndex<InMemPool, Key = Vec<u8>, PKey = Vec<u8>, Value = Vec<u8>>,
    {
        let mem_pool = get_in_mem_pool();
        let c_key = ContainerKey::new(0, 0);
        let index = Arc::new(I::create_with_bucket_num(c_key, mem_pool, 16).unwrap());

        // 1) Insert some entries (key, pkey, ts, tx_id, value)
        index
            .insert(
                b"key1".to_vec(),
                b"pkey1".to_vec(),
                100,
                1,
                b"value1".to_vec(),
            )
            .unwrap();
        index
            .insert(
                b"key2".to_vec(),
                b"pkey2".to_vec(),
                100,
                1,
                b"value2".to_vec(),
            )
            .unwrap();
        index
            .insert(
                b"key1".to_vec(),
                b"pkey3".to_vec(),
                150,
                1,
                b"value3".to_vec(),
            )
            .unwrap();

        // 2) Get an entry at a specific timestamp
        let got = index.get(b"key1", b"pkey1", 100).unwrap();
        assert_eq!(
            got,
            Some(b"value1".to_vec()),
            "Should find value1 at ts=100"
        );

        // 3) Update an existing entry
        index
            .update(
                b"key1".to_vec(),
                b"pkey3".to_vec(),
                151,
                2, // new transaction ID
                b"value3_updated".to_vec(),
            )
            .unwrap();

        // 4) Delete an entry
        // index.delete(b"key2", b"pkey2", 100, 2).unwrap();

        // 5) Now scan at ts=200
        let mut scan_iter = index.scan(200).unwrap();
        let mut scanned = Vec::new();
        while let Some((key, pkey, value)) = scan_iter.next() {
            // println!("Scanned: key={:?}, pkey={:?}, value={:?}", key, pkey, std::str::from_utf8(&value).unwrap());
            scanned.push((key, pkey, value));
        }

        // 6) Verify we see "value1" for key1/pkey1, "value3_updated" for key1/pkey3,
        //    and do *not* see key2/pkey2.
        assert!(
            scanned
                .iter()
                .any(|(k, pk, v)| k == b"key1" && pk == b"pkey1" && v == b"value1"),
            "Should still have key1/pkey1/value1"
        );
        assert!(
            scanned
                .iter()
                .any(|(k, pk, v)| k == b"key1" && pk == b"pkey3" && v == b"value3_updated"),
            "Should see updated value3 for key1/pkey3"
        );
        // assert!(
        //     !scanned
        //         .iter()
        //         .any(|(k, pk, _)| k == b"key2" && pk == b"pkey2"),
        //     "Deleted key2/pkey2 should not appear at ts=200"
        // );
    }

    fn test_basic_index_ops<I>()
    where
        I: MvccIndex<InMemPool, Key = Vec<u8>, PKey = Vec<u8>, Value = Vec<u8>>,
    {
        test_basic_index_ops0::<I>();
        test_simple_insert::<I>();
        test_many_inserts_and_reads::<I>();
        test_simple_update_different_timestamp::<I>();
        test_update_twice::<I>();
        test_insert_and_scan::<I>();
    }

    fn test_delta_scan_op0<I>()
    where
        I: MvccIndex<InMemPool, Key = Vec<u8>, PKey = Vec<u8>, Value = Vec<u8>>,
    {
        let num = 100;
        let mem_pool = get_in_mem_pool();
        let c_key = ContainerKey::new(0, 0);
        let hash_join_table = Arc::new(I::create_with_bucket_num(c_key, mem_pool, 16).unwrap());

        // 0..num inserts
        for i in (0..num).into_iter().step_by(1) {
            let key = format!("key{}", i).into_bytes();
            let pkey = format!("pkey{}", i).into_bytes();
            let value = format!("value{}", i).into_bytes();
            hash_join_table.insert(key, pkey, 1, 1, value).unwrap();
        }

        hash_join_table.split_at_ts(2).unwrap();

        // 0..num updates
        for i in (0..num - 1).into_iter().step_by(1) {
            let key = format!("key{}", i).into_bytes();
            let pkey = format!("pkey{}", i).into_bytes();
            let value = format!("value{}", i + 10000).into_bytes();
            hash_join_table.update(key, pkey, 3, 1, value).unwrap();
        }

        {
            let i = num;
            let key = format!("key{}", i).into_bytes();
            let pkey = format!("pkey{}", i).into_bytes();
            let value = format!("value{}", i).into_bytes();
            hash_join_table.insert(key, pkey, 3, 1, value).unwrap();
        }

        for entry in hash_join_table.delta_scan(1, 3).unwrap() {
            if &entry.0 == format!("key{}", num - 1).as_bytes() {
                // no delta
                assert!(false, "should not exist in delta");
            } else if &entry.0 == format!("key{}", num).as_bytes() {
                assert_eq!(
                    &entry.2,
                    &Delta::Inserted(format!("value{}", num).as_bytes().to_vec())
                );
            } else {
                let i_str = &entry.0[3..];
                let i = std::str::from_utf8(i_str)
                    .unwrap()
                    .parse::<usize>()
                    .unwrap();
                assert_eq!(
                    &entry.2,
                    &Delta::Updated(format!("value{}", i + 10000).as_bytes().to_vec())
                );
            }
        }
    }

    fn test_delta_scan_op1<I>()
    where
        I: MvccIndex<InMemPool, Key = Vec<u8>, PKey = Vec<u8>, Value = Vec<u8>>,
    {
        let num = 100;
        let mem_pool = get_in_mem_pool();
        let c_key = ContainerKey::new(0, 0);
        let hash_join_table = Arc::new(I::create_with_bucket_num(c_key, mem_pool, 16).unwrap());

        // 0..num inserts
        for i in (0..num).into_iter().step_by(1) {
            let key = format!("key{}", i).into_bytes();
            let pkey = format!("pkey{}", i).into_bytes();
            let value = format!("value{}", i).into_bytes();
            hash_join_table.insert(key, pkey, 1, 1, value).unwrap();
        }

        hash_join_table.split_at_ts(2).unwrap();

       // 0..num inserts
        for i in (0..num).into_iter().step_by(1) {
            let key = format!("key{}", i).into_bytes();
            let pkey = format!("pkey{}", i).into_bytes();
            let value = format!("new_value{}", i).into_bytes();
            hash_join_table.update(key, pkey, 3, 1, value).unwrap();
        }

        for entry in hash_join_table.delta_scan(1, 3).unwrap() {
            let i_str = &entry.0[3..];
            let i = std::str::from_utf8(i_str)
                .unwrap()
                .parse::<usize>()
                .unwrap();
            assert_eq!(
                &entry.2,
                &Delta::Updated(format!("new_value{}", i).as_bytes().to_vec())
            );
        }
    }

    fn test_simple_insert<I>()
    where
        I: MvccIndex<InMemPool, Key = Vec<u8>, PKey = Vec<u8>, Value = Vec<u8>>,
    {
        let mem_pool = get_in_mem_pool();
        let c_key = ContainerKey::new(0, 0);
        let hash_join_table = I::create(c_key, mem_pool).unwrap();
        hash_join_table
            .insert(vec![1], vec![1], 1, 1, vec![1])
            .unwrap();
        let get_result = hash_join_table.get(&[2], &[1], 1);
        assert_eq!(get_result.unwrap(), None);

        let get_result = hash_join_table.get(&[1], &[1], 0);
        assert_eq!(get_result.unwrap(), None);

        let get_result = hash_join_table.get(&[1], &[1], 2);
        assert_eq!(get_result.unwrap().unwrap(), &[1]);
    }

    fn test_many_inserts_and_reads<I>()
    where
        I: MvccIndex<InMemPool, Key = Vec<u8>, PKey = Vec<u8>, Value = Vec<u8>>,
    {
        let mem_pool = get_in_mem_pool();
        let c_key = ContainerKey::new(0, 0);
        let hash_join_table = I::create(c_key, mem_pool).unwrap();

        for i in (0..1000).into_iter() {
            let key = format!("key__{}", i).into_bytes();
            let pkey = format!("pkey__{}", i).into_bytes();
            let value = format!("value__{}", i).into_bytes();
            hash_join_table
                .insert(key, pkey, i as u64, 1, value)
                .unwrap();
        }

        // log_warn!("FINISH JOIN!!!!!!!!!!");

        // Verify all entries after insertions are complete
        for i in 0..1000 {
            let key = format!("key__{}", i).into_bytes();
            let pkey = format!("pkey__{}", i).into_bytes();
            let expected_value = format!("value__{}", i).into_bytes();
            let retrieved_val = hash_join_table.get(&key, &pkey, i as u64).unwrap();
            assert_eq!(retrieved_val.unwrap(), expected_value);
        }
    }

    /// no concurrent support
    fn _test_concurrent_inserts_and_reads<I>()
    where
        I: MvccIndex<InMemPool, Key = Vec<u8>, PKey = Vec<u8>, Value = Vec<u8>>,
    {
        use std::thread;

        let mem_pool = get_in_mem_pool();
        let c_key = ContainerKey::new(0, 0);
        let hash_join_table = Arc::new(I::create(c_key, mem_pool).unwrap());

        let hash_join_table_clone = hash_join_table.clone();
        let handle = thread::spawn(move || {
            // Insert entries in a separate thread
            for i in (0..1000).into_iter().step_by(2) {
                let key = format!("key__{}", i).into_bytes();
                let pkey = format!("pkey__{}", i).into_bytes();
                let value = format!("value__{}", i).into_bytes();
                hash_join_table_clone
                    .insert(key, pkey, i as u64, 1, value)
                    .unwrap();
            }
        });

        for i in (1..1000).into_iter().step_by(2) {
            let key = format!("key__{}", i).into_bytes();
            let pkey = format!("pkey__{}", i).into_bytes();
            let value = format!("value__{}", i).into_bytes();
            hash_join_table
                .insert(key, pkey, i as u64, 1, value)
                .unwrap();
        }

        // Read entries while inserts are happening
        for i in 0..1000 {
            let key = format!("key__{}", i).into_bytes();
            let pkey = format!("pkey__{}", i).into_bytes();
            // It's possible that the key hasn't been inserted yet
            let _ = hash_join_table.get(&key, &pkey, i as u64);
        }

        handle.join().unwrap();
        // log_warn!("FINISH JOIN!!!!!!!!!!");
        let hash_join_table_clone = hash_join_table.clone();

        let handle = thread::spawn(move || {
            // Verify all entries after insertions are complete
            for i in 0..1000 {
                let key = format!("key__{}", i).into_bytes();
                let pkey = format!("pkey__{}", i).into_bytes();
                let expected_value = format!("value__{}", i).into_bytes();
                let retrieved_val = hash_join_table_clone.get(&key, &pkey, i as u64).unwrap();
                assert_eq!(retrieved_val.unwrap(), expected_value);
            }
        });
        // Verify all entries after insertions are complete
        for i in 0..1000 {
            // log_warn!("get {i}");
            let key = format!("key__{}", i).into_bytes();
            let pkey = format!("pkey__{}", i).into_bytes();
            let expected_value = format!("value__{}", i).into_bytes();
            let retrieved_val = hash_join_table.get(&key, &pkey, i as u64).unwrap();
            assert_eq!(retrieved_val.unwrap(), expected_value);
        }

        handle.join().unwrap();
    }

    fn test_simple_delete_different_timestamp<I>()
    where
        I: MvccIndex<InMemPool, Key = Vec<u8>, PKey = Vec<u8>, Value = Vec<u8>>,
    {
        let mem_pool = get_in_mem_pool();
        let c_key = ContainerKey::new(0, 0);
        let hash_join_table = Arc::new(I::create(c_key, mem_pool).unwrap());

        hash_join_table
            .insert(vec![1], vec![1], 1, 1, vec![1])
            .unwrap();

        hash_join_table
            .delete(&(vec![1])[..], &(vec![1])[..], 2, 1)
            .unwrap();

        let get_result = hash_join_table.get(&[2], &[1], 1);
        assert_eq!(get_result.unwrap(), None);

        let get_result = hash_join_table.get(&[1], &[1], 0);
        assert_eq!(get_result.unwrap(), None);

        let get_result = hash_join_table.get(&[1], &[1], 2);
        assert_eq!(get_result.unwrap(), None);

        let get_result = hash_join_table.get(&[1], &[1], 1);
        assert_eq!(get_result.unwrap().unwrap(), &[1]);
    }

    fn test_simple_update_different_timestamp<I>()
    where
        I: MvccIndex<InMemPool, Key = Vec<u8>, PKey = Vec<u8>, Value = Vec<u8>>,
    {
        let mem_pool = get_in_mem_pool();
        let c_key = ContainerKey::new(0, 0);
        let hash_join_table = Arc::new(I::create(c_key, mem_pool).unwrap());

        hash_join_table
            .insert(vec![1], vec![1], 1, 1, vec![1])
            .unwrap();

        hash_join_table
            .update(vec![1], vec![1], 2, 1, vec![2])
            .unwrap();

        let get_result = hash_join_table.get(&[2], &[1], 1);
        assert_eq!(get_result.unwrap(), None);

        let get_result = hash_join_table.get(&[1], &[1], 0);
        assert_eq!(get_result.unwrap(), None);

        let get_result = hash_join_table.get(&[1], &[1], 2);
        assert_eq!(get_result.unwrap().unwrap(), &[2]);

        let get_result = hash_join_table.get(&[1], &[1], 1);
        assert_eq!(get_result.unwrap().unwrap(), &[1]);
    }

    #[ignore = "not implemented delete yet"]
    #[test]
    fn test_insert_and_delete_and_scan() {
        let mem_pool = get_in_mem_pool();
        let c_key = ContainerKey::new(0, 0);
        let hash_join_table = Arc::new(LinearHashTable::new_with_bucket_num(c_key, mem_pool, 16));

        // 0..1000 inserts
        for i in (0..1000).into_iter().step_by(1) {
            let key = format!("key{}", i).into_bytes();
            let pkey = format!("pkey{}", i).into_bytes();
            let value = format!("value{}", i).into_bytes();
            hash_join_table.insert(key, pkey, 1, 1, value).unwrap();
        }

        // 0..1000 deletes
        for i in (0..1000).into_iter().step_by(1) {
            let key = format!("key{}", i).into_bytes();
            let pkey = format!("pkey{}", i).into_bytes();
            hash_join_table.delete(&key, &pkey, 2, 1).unwrap();
        }

        let scan_iter = hash_join_table.scan_all().unwrap();
        let mut cnt = 0;
        for pair in scan_iter {
            cnt += 1;
            let (key, pkey, value) = (pair.key, pair.pkey, pair.value);
            assert_eq!(&key[3..], &pkey[4..]);
            assert_eq!(&pkey[4..], &value[5..]);
        }
        assert_eq!(cnt, 1000);
    }

    #[test]
    fn test_insert_and_update_and_scan() {
        let mem_pool = get_in_mem_pool();
        let c_key = ContainerKey::new(0, 0);
        let hash_join_table = Arc::new(LinearHashTable::new_with_bucket_num(c_key, mem_pool, 16));

        // 0..1000 inserts
        for i in (0..1000).into_iter().step_by(1) {
            let key = format!("key{}", i).into_bytes();
            let pkey = format!("pkey{}", i).into_bytes();
            let value = format!("value{}", i).into_bytes();
            hash_join_table.insert(key, pkey, 1, 1, value).unwrap();
        }

        // 0..1000 updates
        for i in (0..1000).into_iter().step_by(1) {
            let key = format!("key{}", i).into_bytes();
            let pkey = format!("pkey{}", i).into_bytes();
            let new_value = format!("new_value{}", i).into_bytes();
            hash_join_table.update(key, pkey, 2, 1, new_value).unwrap();
        }

        let scan_iter = hash_join_table.scan_all().unwrap();
        let mut cnt = 0;
        for pair in scan_iter {
            cnt += 1;
            let (key, pkey, value) = (pair.key, pair.pkey, pair.value);
            assert_eq!(&key[3..], &pkey[4..]);
            if value.starts_with(b"value") {
                assert_eq!(&pkey[4..], &value[5..]);
            } else {
                assert_eq!(&pkey[4..], &value[9..]);
            }
        }
        assert_eq!(cnt, 2000);
    }

    fn test_insert_and_scan<I>()
    where
        I: MvccIndex<InMemPool, Key = Vec<u8>, PKey = Vec<u8>, Value = Vec<u8>>,
    {
        let mem_pool = get_in_mem_pool();
        let c_key = ContainerKey::new(0, 0);
        let hash_join_table = Arc::new(I::create_with_bucket_num(c_key, mem_pool, 16).unwrap());

        // 1..100 inserts
        for i in (0..100).into_iter().step_by(1) {
            let key = format!("key{}", i).into_bytes();
            let pkey = format!("pkey{}", i).into_bytes();
            let value = format!("value{}", i).into_bytes();
            hash_join_table.insert(key, pkey, 1, 1, value).unwrap();
        }

        let scan_iter = hash_join_table.scan_all().unwrap();
        let mut cnt = 0;
        for pair in scan_iter {
            cnt += 1;
            let (key, pkey, value) = (pair.key, pair.pkey, pair.value);
            assert_eq!(&key[3..], &pkey[4..]);
            assert_eq!(&pkey[4..], &value[5..]);
        }
        assert_eq!(cnt, 100);
    }

    fn test_scan_key_vec_ops_0<I>()
    where
        I: MvccIndex<InMemPool, Key = Vec<u8>, PKey = Vec<u8>, Value = Vec<u8>>,
    {
        let mem_pool = get_in_mem_pool();
        let c_key = ContainerKey::new(0, 0);
        let hash_join_table = Arc::new(I::create_with_bucket_num(c_key, mem_pool, 16).unwrap());

        // 1..100 inserts
        for i in (0..100).into_iter().step_by(1) {
            let key = format!("key{}", i).into_bytes();
            let pkey = format!("pkey{}", i + 1000).into_bytes();
            let value = format!("value{}", i + 1000).into_bytes();
            hash_join_table
                .insert(key.clone(), pkey, 1, 1, value)
                .unwrap();

            let pkey = format!("pkey{}", i + 2000).into_bytes();
            let value = format!("value{}", i + 2000).into_bytes();
            hash_join_table.insert(key, pkey, 1, 1, value).unwrap();
        }
        for i in (0..100).into_iter().step_by(1) {
            let key = format!("key{}", i).into_bytes();
            let a = hash_join_table.scan_key_vec(&key, 2);
            let t = a.unwrap();

            assert!(
                t.iter()
                    .any(|x| x.0 == format!("pkey{}", i + 1000).into_bytes()),
                "vec: {:?}",
                t.iter()
                    .map(|e| {
                        (
                            std::str::from_utf8(&e.0[..]).unwrap(),
                            std::str::from_utf8(&e.1[..]).unwrap(),
                        )
                    })
                    .collect::<Vec<_>>()
            );
            assert!(t
                .iter()
                .any(|x| x.0 == format!("pkey{}", i + 2000).into_bytes()));
        }
    }

    fn test_update_twice<I>()
    where
        I: MvccIndex<InMemPool, Key = Vec<u8>, PKey = Vec<u8>, Value = Vec<u8>>,
    {
        // Initialize the hash join table using the MvccIndex trait
        let mem_pool = get_in_mem_pool(); // You need to implement or import this function
        let c_key = ContainerKey::new(0, 0);
        let hash_join_table = I::create_with_bucket_num(c_key, mem_pool.clone(), 16).unwrap();

        let data_num = 1000 as usize;
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
        let value_new_new = value_new
            .clone()
            .into_iter()
            .map(|mut ve| {
                ve.push(233);
                ve
            })
            .collect::<Vec<_>>();

        // BENCH HASH_JOIN_TABLE UPDATE

        // Load data into the hash join table
        for (key, pkey, value) in &data {
            hash_join_table
                .insert(key.clone(), pkey.clone(), 0, 0, value.clone())
                .unwrap();
        }

        {
            let data_clone = data.clone();
            let value_new_clone = value_new.clone();

            for ((key, pkey, _value), new_value) in
                (data_clone).into_iter().zip((value_new_clone).into_iter())
            {
                // UPDATE data
                hash_join_table.update(key, pkey, 1, 0, new_value).unwrap();
            }
        }

        {
            let data_clone = data.clone();
            let value_new_new_clone = value_new_new.clone();

            for ((key, pkey, _value), new_value) in (data_clone)
                .into_iter()
                .zip((value_new_new_clone).into_iter())
            {
                // UPDATE data
                hash_join_table.update(key, pkey, 2, 0, new_value).unwrap();
            }
        }

        for ((key, pkey, _value), new_value) in (&data).iter().zip((&value_new_new).iter()) {
            let a = hash_join_table.get(key, pkey, 2).unwrap();
            assert_eq!(a.as_ref().unwrap(), new_value);
        }
    }

    fn test_scan_key_ops<I>()
    where
        I: MvccIndex<InMemPool, Key = Vec<u8>, PKey = Vec<u8>, Value = Vec<u8>>,
    {
        test_scan_key_ops_0::<I>();
        test_scan_key_vec_ops_0::<I>();
    }

    fn test_scan_key_ops_0<I>()
    where
        I: MvccIndex<InMemPool, Key = Vec<u8>, PKey = Vec<u8>, Value = Vec<u8>>,
    {
        let mem_pool = get_in_mem_pool();
        let c_key = ContainerKey::new(0, 0);
        let hash_join_table = Arc::new(I::create_with_bucket_num(c_key, mem_pool, 16).unwrap());

        // 1..100 inserts
        for i in (0..1000).into_iter() {
            for j in (0..10).into_iter() {
                let key = format!("key{}", i).into_bytes();
                let pkey = format!("pkey{}", i * 100 + j).into_bytes();
                let value = format!("value{}", i * 100 + j).into_bytes();
                hash_join_table.insert(key, pkey, 1, 1, value).unwrap();
            }
        }

        for i in (0..1000).into_iter().step_by(1) {
            let key = format!("key{}", i).into_bytes();
            let a = hash_join_table.scan_key(&key, 2);
            let t = a.unwrap().collect::<Vec<_>>();

            for m in t {
                // log_warn!("{:?} {:?}", String::from_utf8(m.0), String::from_utf8(m.1));
                assert!(
                    m.0[4..].starts_with(format!("{}", i).as_bytes()) || i == 0,
                    "{:?}, i:{i}",
                    m.0
                );
                assert_eq!(m.1[5..], m.0[4..]);
            }
            // log_warn!("{:?} ends -----------------", key);
        }
    }

    #[ignore = "no concurrency support"]
    #[test]
    fn concurrent_inserts_and_update() {
        use std::thread;

        let mem_pool = get_in_mem_pool();
        let c_key = ContainerKey::new(0, 0);
        let hash_join_table = Arc::new(LinearHashTable::new_with_bucket_num(c_key, mem_pool, 16));

        let hash_join_table_clone = hash_join_table.clone();

        // 1..1000 inserts
        for i in (0..1000).into_iter().step_by(1) {
            let key = format!("key{}", i).into_bytes();
            let pkey = format!("pkey{}", i).into_bytes();
            let value = format!("value{}", i).into_bytes();
            hash_join_table.insert(key, pkey, 1, 1, value).unwrap();
        }

        // 1..1000..2 updates
        let handle = thread::spawn(move || {
            // Insert entries in a separate thread
            for i in (1..1000).into_iter().step_by(2) {
                let key = format!("key{}", i).into_bytes();
                let pkey = format!("pkey{}", i).into_bytes();
                let value = format!("value{}", i * 2 + 10000).into_bytes();
                hash_join_table_clone
                    .update(key, pkey, 2, 1, value)
                    .unwrap();
            }
        });

        // Read entries while inserts are happening
        for i in (0..1000).into_iter().step_by(10) {
            let key = format!("key{}", i).into_bytes();
            let pkey = format!("pkey{}", i).into_bytes();
            // It's possible that the key hasn't been inserted yet
            let _ = hash_join_table.get(&key, &pkey, 2);
        }

        // 0..1000..2 updates
        for i in (0..1000).into_iter().step_by(2) {
            let key = format!("key{}", i).into_bytes();
            let pkey = format!("pkey{}", i).into_bytes();
            let value = format!("value{}", i * 2 + 10000).into_bytes();
            hash_join_table
                .update(key, pkey, 2 as u64, 1, value)
                .unwrap();
        }

        handle.join().unwrap();

        let hash_join_table_clone = hash_join_table.clone();

        let handle = thread::spawn(move || {
            // Verify all entries after insertions are complete
            for i in 0..1000 {
                let key = format!("key{}", i).into_bytes();
                let pkey = format!("pkey{}", i).into_bytes();
                let expected_value = format!("value{}", i * 2 + 10000).into_bytes();
                let retrieved_val = hash_join_table_clone.get(&key, &pkey, 2).unwrap();
                assert_eq!(retrieved_val.unwrap(), expected_value);
            }
        });
        // Verify all entries after insertions are complete
        for i in 0..1000 {
            let key = format!("key{}", i).into_bytes();
            let pkey = format!("pkey{}", i).into_bytes();
            let expected_value = format!("value{}", i * 2 + 10000).into_bytes();
            let retrieved_val = hash_join_table.get(&key, &pkey, 2).unwrap();
            assert_eq!(retrieved_val.unwrap(), expected_value);
        }

        handle.join().unwrap();

        for i in 0..1000 {
            let key = format!("key{}", i).into_bytes();
            let pkey = format!("pkey{}", i).into_bytes();
            let expected_value = format!("value{}", i).into_bytes();
            let retrieved_val = hash_join_table.get(&key, &pkey, 1).unwrap();
            assert_eq!(retrieved_val.unwrap(), expected_value);
        }
    }

    #[ignore = "no concurrency and delete support"]
    #[test]
    fn concurrent_inserts_and_delete() {
        use std::thread;

        let mem_pool = get_in_mem_pool();
        let c_key = ContainerKey::new(0, 0);
        let hash_join_table = Arc::new(LinearHashTable::new_with_bucket_num(c_key, mem_pool, 16));

        let hash_join_table_clone = hash_join_table.clone();

        // 0..1000 inserts
        for i in (0..1000).into_iter().step_by(1) {
            let key = format!("key{}", i).into_bytes();
            let pkey = format!("pkey{}", i).into_bytes();
            let value = format!("value{}", i).into_bytes();
            hash_join_table.insert(key, pkey, 1, 1, value).unwrap();
        }

        // 1..1000..2 deletes
        let handle = thread::spawn(move || {
            // Insert entries in a separate thread
            for i in (1..1000).into_iter().step_by(2) {
                let key = format!("key{}", i).into_bytes();
                let pkey = format!("pkey{}", i).into_bytes();
                hash_join_table_clone
                    .delete(&key[..], &pkey[..], 2, 1)
                    .unwrap();
            }
        });

        // Read entries while deletes are happening
        for i in (0..1000).into_iter().step_by(10) {
            let key = format!("key{}", i).into_bytes();
            let pkey = format!("pkey{}", i).into_bytes();
            // It's possible that the key hasn't been inserted yet
            let _ = hash_join_table.get(&key, &pkey, 2);
        }

        // 0..1000..2 deletes
        for i in (0..1000).into_iter().step_by(2) {
            let key = format!("key{}", i).into_bytes();
            let pkey = format!("pkey{}", i).into_bytes();
            hash_join_table.delete(&key, &pkey, 2 as u64, 1).unwrap();
        }

        handle.join().unwrap();

        let hash_join_table_clone = hash_join_table.clone();

        let handle = thread::spawn(move || {
            // Verify all entries after deletes are complete
            for i in 0..1000 {
                let key = format!("key{}", i).into_bytes();
                let pkey = format!("pkey{}", i).into_bytes();
                let get_result = hash_join_table_clone.get(&key, &pkey, 2);
                assert_eq!(get_result.unwrap(), None);
            }
        });
        // Verify all entries after deletes are complete
        for i in 0..1000 {
            let key = format!("key{}", i).into_bytes();
            let pkey = format!("pkey{}", i).into_bytes();
            let get_result = hash_join_table.get(&key, &pkey, 2);
            assert_eq!(get_result.unwrap(), None);
        }

        handle.join().unwrap();

        for i in 0..1000 {
            let key: Vec<u8> = format!("key{}", i).into_bytes();
            let pkey = format!("pkey{}", i).into_bytes();
            let expected_value = format!("value{}", i).into_bytes();
            let get_result = hash_join_table.get(&key, &pkey, 1);
            assert_eq!(get_result.unwrap().unwrap(), expected_value);
        }
    }

    #[ignore = "not implemented delete yet"]
    #[test]
    fn test_garbage_collect() {
        let mem_pool = get_in_mem_pool();
        let c_key = ContainerKey::new(0, 0);
        let hash_join_table = Arc::new(LinearHashTable::new_with_bucket_num(c_key, mem_pool, 16));

        let hash_join_table_clone = hash_join_table.clone();

        // 0..1000 inserts at ts 1
        for i in (0..1000).into_iter().step_by(1) {
            let key = format!("key{}", i).into_bytes();
            let pkey = format!("pkey{}", i).into_bytes();
            let value = format!("value{}", i).into_bytes();
            hash_join_table.insert(key, pkey, 1, 1, value).unwrap();
        }

        // 0..1000 deletes at ts 2
        for i in (0..1000).into_iter().step_by(1) {
            let key = format!("key{}", i).into_bytes();
            let pkey = format!("pkey{}", i).into_bytes();
            hash_join_table_clone
                .delete(&key[..], &pkey[..], 2, 1)
                .unwrap();
        }

        // Verify all entries after deletes are complete
        for i in 0..1000 {
            let key = format!("key{}", i).into_bytes();
            let pkey = format!("pkey{}", i).into_bytes();
            let get_result = hash_join_table_clone.get(&key, &pkey, 2);
            assert_eq!(get_result.unwrap(), None);
        }

        for i in 0..1000 {
            let key: Vec<u8> = format!("key{}", i).into_bytes();
            let pkey = format!("pkey{}", i).into_bytes();
            let expected_value = format!("value{}", i).into_bytes();
            let get_result = hash_join_table.get(&key, &pkey, 1);
            assert_eq!(get_result.unwrap().unwrap(), expected_value);
        }

        let mut scan_all_iter = hash_join_table.scan_all().unwrap();
        let mut item_count = 0;
        while let Some(item) = scan_all_iter.next() {
            item_count += 1;
            log_warn!(
                "item key: {:?}, item pkey: {:?}, item val: {:?}, item start ts: {}, end ts: {}",
                str::from_utf8(&item.key),
                str::from_utf8(&item.pkey),
                str::from_utf8(&item.value),
                item.start_ts,
                item.end_ts
            );
        }
        assert_eq!(item_count, 2000);
        hash_join_table.garbage_collect(1).unwrap();

        let mut scan_all_iter = hash_join_table.scan_all().unwrap();
        let mut item_count = 0;
        while let Some(item) = scan_all_iter.next() {
            item_count += 1;
            // log_warn!(
            //     "item key: {:?}, item pkey: {:?}, item val: {:?}, item start ts: {}, end ts: {}",
            //     str::from_utf8(&item.key),
            //     str::from_utf8(&item.pkey),
            //     str::from_utf8(&item.value),
            //     item.start_ts,
            //     item.end_ts
            // );
        }
        assert_eq!(item_count, 1000);

        hash_join_table.garbage_collect(2).unwrap();

        let mut scan_all_iter = hash_join_table.scan_all().unwrap();
        let mut item_count = 0;
        while let Some(item) = scan_all_iter.next() {
            item_count += 1;
            log_warn!(
                "item key: {:?}, item pkey: {:?}, item val: {:?}, item start ts: {}, end ts: {}",
                str::from_utf8(&item.key),
                str::from_utf8(&item.pkey),
                str::from_utf8(&item.value),
                item.start_ts,
                item.end_ts
            );
        }
        assert_eq!(item_count, 0);
    }
}
