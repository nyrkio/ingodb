use ingodb::{IBlob, LsmConfig, LsmEngine, Value, Filter};

fn make_user(name: &str, age: u64) -> IBlob {
    IBlob::from_pairs(vec![
        ("type", Value::String("user".into())),
        ("name", Value::String(name.into())),
        ("age", Value::U64(age)),
    ])
}

fn make_order(user_hash: ingodb::ContentHash, amount: u64) -> IBlob {
    IBlob::from_pairs(vec![
        ("type", Value::String("order".into())),
        ("user", Value::Hash(user_hash)),
        ("amount", Value::U64(amount)),
    ])
}

fn test_engine() -> (LsmEngine, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let config = LsmConfig {
        data_dir: dir.path().to_path_buf(),
        memtable_size: 8192,
        block_size: 512,
        compaction_threshold: 4,
    };
    let engine = LsmEngine::open(config).unwrap();
    (engine, dir)
}

#[test]
fn test_document_lifecycle() {
    let (engine, _dir) = test_engine();

    // Insert a user
    let user = make_user("Henrik", 42);
    let user_hash = *user.hash();
    engine.put(user).unwrap();

    // Insert orders referencing the user
    let order1 = make_order(user_hash, 100);
    let order2 = make_order(user_hash, 250);
    let order1_hash = *order1.hash();
    let order2_hash = *order2.hash();
    engine.put(order1).unwrap();
    engine.put(order2).unwrap();

    // Retrieve and verify
    let found_user = engine.get(&user_hash).unwrap().unwrap();
    assert_eq!(found_user.get("name"), Some(&Value::String("Henrik".into())));

    let found_order = engine.get(&order1_hash).unwrap().unwrap();
    assert_eq!(found_order.get("user"), Some(&Value::Hash(user_hash)));
    assert_eq!(found_order.get("amount"), Some(&Value::U64(100)));

    // Verify hash references work: follow order → user
    let order2_doc = engine.get(&order2_hash).unwrap().unwrap();
    if let Some(Value::Hash(ref_hash)) = order2_doc.get("user") {
        let referenced_user = engine.get(ref_hash).unwrap().unwrap();
        assert_eq!(
            referenced_user.get("name"),
            Some(&Value::String("Henrik".into()))
        );
    } else {
        panic!("expected Hash reference in order.user field");
    }
}

#[test]
fn test_content_addressable_dedup() {
    let (engine, _dir) = test_engine();

    let blob1 = make_user("Duplicate", 10);
    let blob2 = make_user("Duplicate", 10);
    assert_eq!(blob1.hash(), blob2.hash());

    engine.put(blob1.clone()).unwrap();
    engine.put(blob2).unwrap();

    let found = engine.get(blob1.hash()).unwrap().unwrap();
    assert_eq!(found.get("name"), Some(&Value::String("Duplicate".into())));
}

#[test]
fn test_survive_restart() {
    let dir = tempfile::tempdir().unwrap();
    let config = LsmConfig {
        data_dir: dir.path().to_path_buf(),
        memtable_size: 1024 * 1024,
        block_size: 512,
        compaction_threshold: 4,
    };

    let user = make_user("Persistent", 99);
    let hash = *user.hash();

    {
        let engine = LsmEngine::open(config.clone()).unwrap();
        engine.put(user).unwrap();
        engine.sync().unwrap();
    }

    {
        let engine = LsmEngine::open(config).unwrap();
        let found = engine.get(&hash).unwrap().unwrap();
        assert_eq!(
            found.get("name"),
            Some(&Value::String("Persistent".into()))
        );
    }
}

#[test]
fn test_flush_and_recover_from_sstable() {
    let dir = tempfile::tempdir().unwrap();
    let config = LsmConfig {
        data_dir: dir.path().to_path_buf(),
        memtable_size: 1024 * 1024,
        block_size: 512,
        compaction_threshold: 4,
    };

    let mut hashes = Vec::new();

    {
        let engine = LsmEngine::open(config.clone()).unwrap();
        for i in 0..20 {
            let blob = make_user(&format!("User{i}"), i);
            hashes.push(*blob.hash());
            engine.put(blob).unwrap();
        }
        engine.flush_memtable().unwrap();
    }

    {
        let engine = LsmEngine::open(config).unwrap();
        for hash in &hashes {
            assert!(
                engine.get(hash).unwrap().is_some(),
                "document not found after restart"
            );
        }
    }
}

#[test]
fn test_zero_copy_field_extraction() {
    let blob = make_user("ZeroCopy", 77);
    let encoded = blob.encode();

    let name = IBlob::extract_field(&encoded, "name").unwrap();
    assert_eq!(name, Value::String("ZeroCopy".into()));

    let age = IBlob::extract_field(&encoded, "age").unwrap();
    assert_eq!(age, Value::U64(77));
}

#[test]
fn test_filter_evaluation() {
    let blob = make_user("FilterTest", 30);
    let get_field = |name: &str| blob.get(name).cloned();

    let filter = Filter::Gt {
        field: "age".into(),
        value: Value::U64(25),
    };
    assert!(filter.matches(&get_field));

    let compound = Filter::And(vec![
        Filter::Eq {
            field: "name".into(),
            value: Value::String("FilterTest".into()),
        },
        Filter::Lt {
            field: "age".into(),
            value: Value::U64(50),
        },
    ]);
    assert!(compound.matches(&get_field));
}

#[test]
fn test_large_documents() {
    let (engine, _dir) = test_engine();

    let keys: Vec<String> = (0..100).map(|i| format!("field_{i:04}")).collect();
    let pairs: Vec<(&str, Value)> = keys
        .iter()
        .enumerate()
        .map(|(i, k)| (k.as_str(), Value::U64(i as u64)))
        .collect();
    let blob = IBlob::from_pairs(pairs);
    let hash = *blob.hash();

    engine.put(blob).unwrap();
    let found = engine.get(&hash).unwrap().unwrap();
    assert_eq!(found.field_count(), 100);
}

#[test]
fn test_high_throughput_writes() {
    let (engine, _dir) = test_engine();

    let mut hashes = Vec::with_capacity(1000);
    for i in 0..1000u64 {
        let blob = IBlob::from_pairs(vec![
            ("seq", Value::U64(i)),
            ("payload", Value::Bytes(vec![0xAB; 64])),
        ]);
        hashes.push(*blob.hash());
        engine.put(blob).unwrap();
    }

    for hash in &hashes {
        assert!(engine.get(hash).unwrap().is_some());
    }

    assert!(
        engine.sstable_count() >= 1,
        "expected SSTables after 1000 writes"
    );
}
