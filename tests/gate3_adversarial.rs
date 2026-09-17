//! Gate 3 evidence for supported headless operations. Each test owns its own
//! temporary identity space and does not exercise product capabilities.

use std::{fs, sync::Barrier, thread, time::Duration};

use genesis_core::{
    CoreError, CoreState, IndexSyncState, Mutation, PropertyFilter, PropertyQuery, PropertySort,
    PropertyValue, RelationshipDirection, GENESIS_METADATA_DIRECTORY, RECORD_STORE_FILENAME,
};

#[test]
fn repeated_mutation_and_reopen_keeps_record_revision_and_index_in_agreement() {
    let library = tempfile::tempdir().expect("temporary library");
    let mut core = CoreState::open(library.path()).expect("open Core");
    let source = core.create_node().expect("source");
    let target = core.create_node().expect("target");
    let mut last_revision = core
        .status()
        .expect("initial status")
        .index_sync
        .record_revision;

    for iteration in 0..8 {
        let changed = core
            .set_property(source.id, "counter", PropertyValue::Integer(iteration))
            .expect("canonical update");
        assert_eq!(changed.revision, last_revision + 1);
        last_revision = changed.revision;

        let linked = core
            .add_relationship(source.id, "links", target.id)
            .expect("relationship mutation");
        assert_eq!(linked.revision, last_revision + 1);
        last_revision = linked.revision;

        let rejected = core.apply_batch(&[
            Mutation::SetProperty {
                node_id: source.id,
                key: "rolled_back".into(),
                value: PropertyValue::Boolean(true),
            },
            Mutation::RemoveRelationship {
                source: target.id,
                key: "missing".into(),
                target: source.id,
            },
        ]);
        assert!(matches!(rejected, Err(CoreError::NotFound { .. })));
        assert_eq!(
            core.get_property(source.id, "rolled_back")
                .expect("rolled-back fact"),
            None
        );
        assert_eq!(
            core.status()
                .expect("post-rollback status")
                .index_sync
                .record_revision,
            last_revision
        );

        let unlinked = core
            .remove_relationship(source.id, "links", target.id)
            .expect("relationship removal");
        assert_eq!(unlinked.revision, last_revision + 1);
        last_revision = unlinked.revision;

        core.close().expect("close Core");
        core = CoreState::open(library.path()).expect("reopen Core");
        let status = core.status().expect("reopened status");
        assert_eq!(status.index_sync.record_revision, last_revision);
        assert_eq!(status.index_sync.applied_revision, last_revision);
        assert_eq!(status.index_sync.state, IndexSyncState::Current);
        assert_eq!(
            core.get_property(source.id, "counter")
                .expect("canonical Property"),
            Some(PropertyValue::Integer(iteration))
        );
        assert!(core
            .query_relationships(source.id, RelationshipDirection::Outgoing, Some("links"))
            .expect("canonical Relationships")
            .is_empty());
        assert_eq!(
            core.query_properties(&PropertyQuery {
                key: "counter".into(),
                filter: Some(PropertyFilter::Equals(PropertyValue::Integer(iteration))),
                sort: PropertySort::Ascending,
                limit: 10,
            })
            .expect("derived Index query")
            .len(),
            1
        );
    }
}

#[test]
fn concurrent_calls_on_one_core_instance_preserve_all_commits() {
    let library = tempfile::tempdir().expect("temporary library");
    let core = CoreState::open(library.path()).expect("open Core");
    let node = core.create_node().expect("Node");
    let first_revision = core.status().expect("status").index_sync.record_revision;
    let start = Barrier::new(6);

    thread::scope(|scope| {
        for worker in 0..4 {
            let start = &start;
            let core = &core;
            scope.spawn(move || {
                start.wait();
                for ordinal in 0..12 {
                    core.set_property(
                        node.id,
                        format!("worker_{worker}_{ordinal}"),
                        PropertyValue::Integer(ordinal),
                    )
                    .expect("concurrent canonical mutation");
                }
            });
        }
        for _ in 0..2 {
            let start = &start;
            let core = &core;
            scope.spawn(move || {
                start.wait();
                for _ in 0..24 {
                    assert_eq!(core.get_node(node.id).expect("concurrent read").id, node.id);
                    core.list_properties(node.id)
                        .expect("concurrent Property read");
                }
            });
        }
    });

    assert_eq!(core.list_properties(node.id).expect("all facts").len(), 48);
    let status = core.status().expect("status after contention");
    assert_eq!(status.index_sync.record_revision, first_revision + 48);
    assert_eq!(
        status.index_sync.applied_revision,
        status.index_sync.record_revision
    );
    core.close().expect("close after contention");
    let reopened = CoreState::open(library.path()).expect("reopen after contention");
    assert_eq!(
        reopened
            .list_properties(node.id)
            .expect("durable facts")
            .len(),
        48
    );
}

#[test]
fn record_writer_contention_preserves_read_and_eventual_commit() {
    let library = tempfile::tempdir().expect("temporary library");
    let core = CoreState::open(library.path()).expect("open Core");
    let node = core.create_node().expect("Node");
    let first_revision = core
        .status()
        .expect("before contention")
        .index_sync
        .record_revision;
    let record_path = library
        .path()
        .join(GENESIS_METADATA_DIRECTORY)
        .join(RECORD_STORE_FILENAME);
    let lock_holder = rusqlite::Connection::open(record_path).expect("independent writer");
    lock_holder
        .execute_batch("BEGIN IMMEDIATE;")
        .expect("hold canonical write lock");
    assert_eq!(
        core.get_node(node.id).expect("read during lock").id,
        node.id
    );

    thread::scope(|scope| {
        let pending =
            scope.spawn(|| core.set_property(node.id, "after_lock", PropertyValue::Boolean(true)));
        thread::sleep(Duration::from_millis(100));
        lock_holder
            .execute_batch("ROLLBACK;")
            .expect("release independent writer without canonical changes");
        assert_eq!(
            pending
                .join()
                .expect("writer thread")
                .expect("commit after lock")
                .revision,
            first_revision + 1
        );
    });

    assert_eq!(
        core.get_property(node.id, "after_lock")
            .expect("committed Property"),
        Some(PropertyValue::Boolean(true))
    );
    core.close().expect("close");
    let reopened = CoreState::open(library.path()).expect("reopen after contention");
    assert_eq!(
        reopened
            .get_property(node.id, "after_lock")
            .expect("durable Property"),
        Some(PropertyValue::Boolean(true))
    );
}

#[test]
fn closed_record_copy_rebuilds_index_and_preserves_retired_identity() {
    let source = tempfile::tempdir().expect("source identity space");
    let destination = tempfile::tempdir().expect("copied identity space");
    let core = CoreState::open(source.path()).expect("open source Core");
    let current = core.create_node().expect("current Node");
    let retired = core.create_node().expect("retired Node");
    core.set_property(current.id, "retained", PropertyValue::Text("yes".into()))
        .expect("canonical fact");
    core.tombstone_node(retired.id).expect("tombstone");
    core.purge_node(retired.id).expect("confirmed purge");
    core.close().expect("close before copying canonical Store");

    let destination_metadata = destination.path().join(GENESIS_METADATA_DIRECTORY);
    fs::create_dir_all(&destination_metadata).expect("create destination metadata");
    fs::copy(
        source
            .path()
            .join(GENESIS_METADATA_DIRECTORY)
            .join(RECORD_STORE_FILENAME),
        destination_metadata.join(RECORD_STORE_FILENAME),
    )
    .expect("copy closed Record Store without Index/recovery artifacts");

    let copied = CoreState::open(destination.path()).expect("open moved Core identity space");
    assert_eq!(
        copied.get_node(current.id).expect("permanent ID").id,
        current.id
    );
    assert_eq!(
        copied
            .get_property(current.id, "retained")
            .expect("canonical Property"),
        Some(PropertyValue::Text("yes".into()))
    );
    assert!(matches!(
        copied.get_node(retired.id),
        Err(CoreError::NotFound { .. })
    ));
    assert!(matches!(
        copied.create_node_with_id(retired.id),
        Err(CoreError::AlreadyExists { .. })
    ));
    copied
        .rebuild_index()
        .expect("explicit derived Index rebuild");
    assert!(matches!(
        copied.create_node_with_id(retired.id),
        Err(CoreError::AlreadyExists { .. })
    ));
}

#[test]
fn missing_issued_id_schema_without_recovery_is_rejected() {
    let library = tempfile::tempdir().expect("temporary library");
    let core = CoreState::open(library.path()).expect("open Core");
    core.create_node().expect("issued Node");
    core.close().expect("close before controlled corruption");
    let metadata = library.path().join(GENESIS_METADATA_DIRECTORY);
    let record_path = metadata.join(RECORD_STORE_FILENAME);
    let connection = rusqlite::Connection::open(&record_path).expect("open isolated fixture");
    connection
        .execute("DROP TABLE issued_node_ids", [])
        .expect("remove required schema element");
    drop(connection);
    fs::remove_dir_all(metadata.join("recovery"))
        .expect("remove only isolated fixture recovery material");

    assert!(CoreState::open(library.path()).is_err());
    let check = rusqlite::Connection::open(&record_path).expect("recheck source fixture");
    let nodes: i64 = check
        .query_row("SELECT COUNT(*) FROM nodes", [], |row| row.get(0))
        .expect("existing canonical Node still present");
    assert_eq!(
        nodes, 1,
        "rejected open must not rewrite surviving Node truth"
    );
}
