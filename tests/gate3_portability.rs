//! Closed-Store relocation evidence. This is intentionally not a live-WAL
//! copy, power-loss, or cross-operating-system filesystem test.

use std::fs;

use genesis_core::{
    CoreError, CoreState, LifecycleState, PropertyValue, RelationshipDirection,
    GENESIS_METADATA_DIRECTORY, RECORD_STORE_FILENAME,
};

#[test]
fn closed_canonical_store_relocates_without_absolute_paths_or_derived_index() {
    let source = tempfile::tempdir().expect("isolated source");
    let destination = tempfile::tempdir().expect("independent destination");
    let core = CoreState::open(source.path()).expect("headless source Core");
    let active = core.create_node().expect("active Node");
    let tombstoned = core.create_node().expect("future tombstone");
    let retired = core.create_node().expect("future retired ID");
    core.set_property(active.id, "name", PropertyValue::Text("portable".into()))
        .expect("active Property");
    core.set_property(tombstoned.id, "count", PropertyValue::Integer(11))
        .expect("tombstoned Property");
    core.add_relationship(active.id, "link", tombstoned.id)
        .expect("canonical Relationship");
    core.add_relationship(active.id, "historical", retired.id)
        .expect("independent Relationship surviving endpoint purge");
    core.tombstone_node(tombstoned.id).expect("tombstone");
    core.tombstone_node(retired.id).expect("prepare purge");
    core.purge_node(retired.id).expect("retire ID");
    let original_revision = core
        .status()
        .expect("original revision")
        .index_sync
        .record_revision;
    core.close()
        .expect("clean shutdown before single-file copy");

    let moved_meta = destination.path().join(GENESIS_METADATA_DIRECTORY);
    fs::create_dir_all(&moved_meta).expect("destination metadata directory");
    fs::copy(
        source
            .path()
            .join(GENESIS_METADATA_DIRECTORY)
            .join(RECORD_STORE_FILENAME),
        moved_meta.join(RECORD_STORE_FILENAME),
    )
    .expect("copy closed Record, no Index/recovery/product state");

    let moved = CoreState::open(destination.path()).expect("open relocated headless Core");
    assert_eq!(
        moved
            .get_node(active.id)
            .expect("active ID")
            .lifecycle_state,
        LifecycleState::Active
    );
    assert_eq!(
        moved
            .get_node(tombstoned.id)
            .expect("tombstone ID")
            .lifecycle_state,
        LifecycleState::Tombstoned
    );
    assert_eq!(
        moved
            .get_property(active.id, "name")
            .expect("active Property"),
        Some(PropertyValue::Text("portable".into()))
    );
    assert_eq!(
        moved
            .get_property(tombstoned.id, "count")
            .expect("tombstoned Property"),
        Some(PropertyValue::Integer(11))
    );
    let relationships = moved
        .query_relationships(active.id, RelationshipDirection::Outgoing, Some("link"))
        .expect("canonical link");
    assert_eq!(relationships.len(), 1);
    assert_eq!(relationships[0].target, tombstoned.id);
    assert_eq!(
        moved
            .query_relationships(
                active.id,
                RelationshipDirection::Outgoing,
                Some("historical")
            )
            .expect("retired endpoint remains historically resolvable")[0]
            .target,
        retired.id
    );
    assert!(matches!(
        moved.get_node(retired.id),
        Err(CoreError::NotFound { .. })
    ));
    assert!(matches!(
        moved.create_node_with_id(retired.id),
        Err(CoreError::AlreadyExists { .. })
    ));
    moved.rebuild_index().expect("rebuild Index at new path");
    assert_eq!(
        moved
            .status()
            .expect("unchanged truth")
            .index_sync
            .record_revision,
        original_revision
    );
    moved.close().expect("close relocated Core");
    let reopened =
        CoreState::open(destination.path()).expect("reopen relocated Core independently");
    assert_eq!(
        reopened
            .get_node(tombstoned.id)
            .expect("durable lifecycle")
            .lifecycle_state,
        LifecycleState::Tombstoned
    );
    assert!(matches!(
        reopened.create_node_with_id(retired.id),
        Err(CoreError::AlreadyExists { .. })
    ));
}
