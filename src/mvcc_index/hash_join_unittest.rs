#[cfg(test)]
mod test_ops {
    use crate::bp::{get_in_mem_pool, ContainerKey, InMemPool};

    use crate::log_warn;
    use crate::mvcc_index::hash_heap::hash_heap_table::HeapHashTable;
    use crate::mvcc_index::hash_join::chained_hash_table::ChainedHashTable;
    use crate::mvcc_index::linear_hash::linear_hash_table::linear_hash_table::LinearHashTable;
    use crate::mvcc_index::ts_partitioned::ts_partitioned_table::TsPartitionedTable;
    use crate::mvcc_index::MvccIndex;
    use crate::page::{Page, PageId, AVAILABLE_PAGE_SIZE};
    use core::str;
    use std::marker::PhantomData;
    use std::sync::Arc;

    fn space_need(key: &[u8], pkey: &[u8], val: &[u8]) -> u32 {
        (16 + val.len()) as u32
    }

    #[test]
    fn test_hash_tables() {
        test_basic_index_ops::<LinearHashTable<_>>();
        test_basic_index_ops::<ChainedHashTable<_>>();
        test_basic_index_ops::<TsPartitionedTable<_>>();
        test_basic_index_ops::<HeapHashTable<_>>();
    }

    fn test_basic_index_ops<I>()
    where
        I: MvccIndex<InMemPool, Key = Vec<u8>, PKey = Vec<u8>, Value = Vec<u8>>,
    {
        test_simple_insert::<I>();
        test_many_inserts_until_rehash::<I>();
        test_many_inserts_and_reads::<I>();
        test_concurrent_inserts_and_reads::<I>();
        test_simple_update_different_timestamp::<I>();
        test_simple_delete_different_timestamp::<I>();
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

    #[test]
    fn simple_insert() {
        let mem_pool = get_in_mem_pool();
        let c_key = ContainerKey::new(0, 0);
        let hash_join_table = LinearHashTable::create(c_key, mem_pool).unwrap();
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

    fn test_many_inserts_until_rehash<I>()
    where
        I: MvccIndex<InMemPool, Key = Vec<u8>, PKey = Vec<u8>, Value = Vec<u8>>,
    {
        let mem_pool = get_in_mem_pool();
        let c_key = ContainerKey::new(0, 0);
        let hash_join_table = I::create(c_key, mem_pool).unwrap();

        let pair_space_need = space_need(
            &format!("{:06}", 1).as_bytes().to_vec(),
            &format!("{:06}", 1).as_bytes().to_vec(),
            &format!("{:06}", 1).as_bytes().to_vec(),
        );
        let pairs_num_rehash = AVAILABLE_PAGE_SIZE as u32 / pair_space_need + 2;
        for i in 0..pairs_num_rehash {
            hash_join_table
                .insert(
                    format!("{:06}", i).as_bytes().to_vec(),
                    format!("{:06}", i).as_bytes().to_vec(),
                    1,
                    1,
                    format!("{:06}", i).as_bytes().to_vec(),
                )
                .unwrap();
        }

        for i in 0..pairs_num_rehash {
            let get_result = hash_join_table.get(
                &(format!("{:06}", i).as_bytes().to_vec())[..],
                &(format!("{:06}", i).as_bytes().to_vec())[..],
                1,
            );
            assert_eq!(
                get_result.unwrap().unwrap(),
                format!("{:06}", i).as_bytes().to_vec()
            );
        }
    }

    #[test]
    fn many_inserts_until_rehash() {
        let mem_pool = get_in_mem_pool();
        let c_key = ContainerKey::new(0, 0);
        let hash_join_table = LinearHashTable::new_with_bucket_num(c_key, mem_pool, 16);

        let pair_space_need = space_need(
            &format!("{:06}", 1).as_bytes().to_vec(),
            &format!("{:06}", 1).as_bytes().to_vec(),
            &format!("{:06}", 1).as_bytes().to_vec(),
        );
        let pairs_num_rehash = AVAILABLE_PAGE_SIZE as u32 / pair_space_need + 2;
        for i in 0..pairs_num_rehash {
            hash_join_table
                .insert(
                    format!("{:06}", i).as_bytes().to_vec(),
                    format!("{:06}", i).as_bytes().to_vec(),
                    1,
                    1,
                    format!("{:06}", i).as_bytes().to_vec(),
                )
                .unwrap();
        }

        for i in 0..pairs_num_rehash {
            let get_result = hash_join_table.get(
                &(format!("{:06}", i).as_bytes().to_vec())[..],
                &(format!("{:06}", i).as_bytes().to_vec())[..],
                1,
            );
            assert_eq!(
                get_result.unwrap().unwrap(),
                format!("{:06}", i).as_bytes().to_vec()
            );
        }
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

    #[test]
    fn many_inserts_and_reads() {
        let mem_pool = get_in_mem_pool();
        let c_key = ContainerKey::new(0, 0);
        let hash_join_table = Arc::new(LinearHashTable::new_with_bucket_num(c_key, mem_pool, 16));

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
            log_warn!("get {i}");
            let key = format!("key__{}", i).into_bytes();
            let pkey = format!("pkey__{}", i).into_bytes();
            let expected_value = format!("value__{}", i).into_bytes();
            let retrieved_val = hash_join_table.get(&key, &pkey, i as u64).unwrap();
            assert_eq!(retrieved_val.unwrap(), expected_value);
        }
    }

    fn test_concurrent_inserts_and_reads<I>()
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

    #[test]
    fn concurrent_inserts_and_reads() {
        use std::thread;

        let mem_pool = get_in_mem_pool();
        let c_key = ContainerKey::new(0, 0);
        let hash_join_table = Arc::new(LinearHashTable::new_with_bucket_num(c_key, mem_pool, 16));

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
            log_warn!("get {i}");
            let key = format!("key__{}", i).into_bytes();
            let pkey = format!("pkey__{}", i).into_bytes();
            let expected_value = format!("value__{}", i).into_bytes();
            let retrieved_val = hash_join_table.get(&key, &pkey, i as u64).unwrap();
            assert_eq!(retrieved_val.unwrap(), expected_value);
        }

        handle.join().unwrap();
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

    #[test]
    fn simple_update_different_timestamp() {
        let mem_pool = get_in_mem_pool();
        let c_key = ContainerKey::new(0, 0);
        let hash_join_table = LinearHashTable::create(c_key, mem_pool).unwrap();
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

    #[test]
    fn simple_delete_different_timestamp() {
        let mem_pool = get_in_mem_pool();
        let c_key = ContainerKey::new(0, 0);
        let hash_join_table = LinearHashTable::create(c_key, mem_pool).unwrap();
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

        // 0..1000 deletes
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

    #[test]
    fn test_insert_and_scan() {
        let mem_pool = get_in_mem_pool();
        let c_key = ContainerKey::new(0, 0);
        let hash_join_table = Arc::new(LinearHashTable::new_with_bucket_num(c_key, mem_pool, 16));

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

    #[ignore = "not implemented yet"]
    #[test]
    fn test_insert_and_get_keys() {
        let mem_pool = get_in_mem_pool();
        let c_key = ContainerKey::new(0, 0);
        let hash_join_table = Arc::new(LinearHashTable::new_with_bucket_num(c_key, mem_pool, 16));

        // 1..100 inserts
        for i in (0..100).into_iter().step_by(1) {
            let key = format!("key{}", i).into_bytes();
            let pkey = format!("pkey{}", i).into_bytes();
            let value = format!("value{}", i).into_bytes();
            hash_join_table.insert(key, pkey, 1, 1, value).unwrap();
        }
        for i in (0..100).into_iter().step_by(1) {
            let key = format!("key{}", i).into_bytes();
            let a = hash_join_table.scan_key(&key, 2);
            let t = a.unwrap().collect::<Vec<_>>();

            for m in t {
                log_warn!("{:?} {:?}", String::from_utf8(m.0), String::from_utf8(m.1));
            }
        }
    }

    #[test]
    fn test_double_update() {
        // Initialize the hash join table using the MvccIndex trait
        let mem_pool = get_in_mem_pool(); // You need to implement or import this function
        let c_key = ContainerKey::new(0, 0);
        let hash_join_table = LinearHashTable::create(c_key, mem_pool.clone()).unwrap();

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

    #[ignore = "not implemented yet"]
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

    #[test]
    fn test_scan_key() {
        let mem_pool = get_in_mem_pool();
        let c_key = ContainerKey::new(0, 0);
        let hash_join_table = Arc::new(LinearHashTable::new_with_bucket_num(c_key, mem_pool, 16));

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
}
