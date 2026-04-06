use ingodb::{DocumentId, IBlob, LsmConfig, LsmEngine, Value, Filter};

fn make_user(name: &str, age: u64) -> IBlob {
    IBlob::from_pairs(vec![
        ("type", Value::String("user".into())),
        ("name", Value::String(name.into())),
        ("age", Value::U64(age)),
    ])
}

fn make_order(user_id: DocumentId, amount: u64) -> IBlob {
    IBlob::from_pairs(vec![
        ("type", Value::String("order".into())),
        ("user", Value::Ref(user_id)),
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
        scaling_parameter: 0,
    };
    let engine = LsmEngine::open(config).unwrap();
    (engine, dir)
}

#[test]
fn test_document_lifecycle() {
    let (engine, _dir) = test_engine();

    // Insert a user
    let user = make_user("Henrik", 42);
    let user_id = *user.id();
    engine.put(user).unwrap();

    // Insert orders referencing the user by stable _id
    let order1 = make_order(user_id, 100);
    let order2 = make_order(user_id, 250);
    let order1_id = *order1.id();
    let order2_id = *order2.id();
    engine.put(order1).unwrap();
    engine.put(order2).unwrap();

    // Retrieve and verify
    let found_user = engine.get(&user_id).unwrap().unwrap();
    assert_eq!(found_user.get("name"), Some(&Value::String("Henrik".into())));
    assert!(!found_user.version().is_nil(), "version should be stamped");

    let found_order = engine.get(&order1_id).unwrap().unwrap();
    assert_eq!(found_order.get("user"), Some(&Value::Ref(user_id)));
    assert_eq!(found_order.get("amount"), Some(&Value::U64(100)));

    // Verify references work: follow order → user via stable _id
    let order2_doc = engine.get(&order2_id).unwrap().unwrap();
    if let Some(Value::Ref(ref_id)) = order2_doc.get("user") {
        let referenced_user = engine.get(ref_id).unwrap().unwrap();
        assert_eq!(
            referenced_user.get("name"),
            Some(&Value::String("Henrik".into()))
        );
    } else {
        panic!("expected Ref in order.user field");
    }
}

#[test]
fn test_same_content_different_ids() {
    let (engine, _dir) = test_engine();

    // Same content but different _ids are independent documents
    let blob1 = make_user("Duplicate", 10);
    let blob2 = make_user("Duplicate", 10);
    assert_ne!(blob1.id(), blob2.id(), "different docs -> different _ids");
    assert_ne!(blob1.hash(), blob2.hash(), "different _ids -> different hashes");

    let id1 = *blob1.id();
    let id2 = *blob2.id();

    engine.put(blob1).unwrap();
    engine.put(blob2).unwrap();

    // Both are independently retrievable
    let found1 = engine.get(&id1).unwrap().unwrap();
    let found2 = engine.get(&id2).unwrap().unwrap();
    assert_eq!(found1.get("name"), Some(&Value::String("Duplicate".into())));
    assert_eq!(found2.get("name"), Some(&Value::String("Duplicate".into())));
}

#[test]
fn test_upsert() {
    let (engine, _dir) = test_engine();

    let id = DocumentId::new();
    let blob1 = IBlob::with_id(id, [
        ("name".into(), Value::String("Henrik".into())),
        ("age".into(), Value::U64(42)),
    ].into());
    engine.put(blob1).unwrap();
    let v1 = *engine.get(&id).unwrap().unwrap().version();

    // Update: same _id, different content
    let blob2 = IBlob::with_id(id, [
        ("name".into(), Value::String("Henrik".into())),
        ("age".into(), Value::U64(43)),
    ].into());
    engine.put(blob2).unwrap();

    let found = engine.get(&id).unwrap().unwrap();
    assert_eq!(found.get("age"), Some(&Value::U64(43)), "update applied");
    assert!(found.version() > &v1, "version advanced");
    assert_eq!(found.id(), &id, "_id unchanged");
}

#[test]
fn test_version_server_assigned() {
    let (engine, _dir) = test_engine();

    let blob = make_user("Test", 1);
    assert!(blob.version().is_nil(), "version nil before put");

    let id = *blob.id();
    engine.put(blob).unwrap();

    let found = engine.get(&id).unwrap().unwrap();
    assert!(!found.version().is_nil(), "version stamped after put");
}

#[test]
fn test_survive_restart() {
    let dir = tempfile::tempdir().unwrap();
    let config = LsmConfig {
        data_dir: dir.path().to_path_buf(),
        memtable_size: 1024 * 1024,
        block_size: 512,
        compaction_threshold: 4,
        scaling_parameter: 0,
    };

    let user = make_user("Persistent", 99);
    let id = *user.id();

    {
        let engine = LsmEngine::open(config.clone()).unwrap();
        engine.put(user).unwrap();
        engine.sync().unwrap();
    }

    {
        let engine = LsmEngine::open(config).unwrap();
        let found = engine.get(&id).unwrap().unwrap();
        assert_eq!(
            found.get("name"),
            Some(&Value::String("Persistent".into()))
        );
        assert!(!found.version().is_nil(), "version survives restart");
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
        scaling_parameter: 0,
    };

    let mut ids = Vec::new();

    {
        let engine = LsmEngine::open(config.clone()).unwrap();
        for i in 0..20 {
            let blob = make_user(&format!("User{i}"), i);
            ids.push(*blob.id());
            engine.put(blob).unwrap();
        }
        engine.flush_memtable().unwrap();
    }

    {
        let engine = LsmEngine::open(config).unwrap();
        for id in &ids {
            assert!(
                engine.get(id).unwrap().is_some(),
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
    let id = *blob.id();

    engine.put(blob).unwrap();
    let found = engine.get(&id).unwrap().unwrap();
    assert_eq!(found.field_count(), 100);
}

#[test]
fn test_high_throughput_writes() {
    let (engine, _dir) = test_engine();

    let mut ids = Vec::with_capacity(1000);
    for i in 0..1000u64 {
        let blob = IBlob::from_pairs(vec![
            ("seq", Value::U64(i)),
            ("payload", Value::Bytes(vec![0xAB; 64])),
        ]);
        ids.push(*blob.id());
        engine.put(blob).unwrap();
    }

    for id in &ids {
        assert!(engine.get(id).unwrap().is_some());
    }

    assert!(
        engine.sstable_count() >= 1,
        "expected SSTables after 1000 writes"
    );
}

#[test]
fn test_delete_and_get() {
    let (engine, _dir) = test_engine();

    let user = make_user("ToDelete", 25);
    let id = *user.id();
    engine.put(user).unwrap();
    assert!(engine.get(&id).unwrap().is_some());

    engine.delete(&id).unwrap();
    assert!(engine.get(&id).unwrap().is_none());
    assert!(!engine.contains(&id).unwrap());
}

#[test]
fn test_delete_survives_flush() {
    let (engine, _dir) = test_engine();

    let user = make_user("FlushDelete", 30);
    let id = *user.id();
    engine.put(user).unwrap();
    engine.flush_memtable().unwrap();

    engine.delete(&id).unwrap();
    engine.flush_memtable().unwrap();

    // After both flushes, the tombstone in the newer SSTable should shadow the live entry
    assert!(engine.get(&id).unwrap().is_none());
}

#[test]
fn test_delete_survives_restart() {
    let dir = tempfile::tempdir().unwrap();
    let config = LsmConfig {
        data_dir: dir.path().to_path_buf(),
        memtable_size: 1024 * 1024,
        block_size: 512,
        compaction_threshold: 100,
        scaling_parameter: 0,
    };

    let user = make_user("RestartDelete", 40);
    let id = *user.id();

    {
        let engine = LsmEngine::open(config.clone()).unwrap();
        engine.put(user).unwrap();
        engine.delete(&id).unwrap();
        engine.sync().unwrap();
    }

    {
        let engine = LsmEngine::open(config).unwrap();
        assert!(engine.get(&id).unwrap().is_none(), "delete survives restart");
    }
}

#[test]
fn test_delete_with_graph_ref_stable() {
    let (engine, _dir) = test_engine();

    // User and order referencing user by _id
    let user = make_user("Henrik", 42);
    let user_id = *user.id();
    engine.put(user).unwrap();

    let order = make_order(user_id, 100);
    let order_id = *order.id();
    engine.put(order).unwrap();

    // Delete user — order's Ref still points to the user_id
    engine.delete(&user_id).unwrap();
    assert!(engine.get(&user_id).unwrap().is_none());

    // Order still exists, reference is intact (it's a dangling ref now, but stable)
    let found_order = engine.get(&order_id).unwrap().unwrap();
    assert_eq!(found_order.get("user"), Some(&Value::Ref(user_id)));

    // Re-insert user — order's ref resolves again
    let user2 = IBlob::with_id(user_id, [
        ("type".into(), Value::String("user".into())),
        ("name".into(), Value::String("Henrik v2".into())),
        ("age".into(), Value::U64(43)),
    ].into());
    engine.put(user2).unwrap();
    let found_user = engine.get(&user_id).unwrap().unwrap();
    assert_eq!(found_user.get("name"), Some(&Value::String("Henrik v2".into())));
}
