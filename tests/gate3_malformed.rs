//! Controlled malformed canonical-store fixtures under the owner-reviewed
//! identity-history and degraded-evidence contract. Recovery artifacts are
//! removed only in isolated temporary libraries so independent reconstruction
//! cannot be mistaken for local validation.

use std::{fs, path::Path};

use genesis_core::{
    CoreState, NodeId, PropertyValue, GENESIS_METADATA_DIRECTORY, RECORD_STORE_FILENAME,
};
use rusqlite::Connection;

fn fixture() -> (tempfile::TempDir, NodeId) {
    let library = tempfile::tempdir().expect("isolated library");
    let core = CoreState::open(library.path()).expect("initialize Core");
    let source = core.create_node().expect("source");
    let target = core.create_node().expect("target");
    core.set_property(source.id, "fact", PropertyValue::Integer(7))
        .expect("seed Property");
    core.add_relationship(source.id, "edge", target.id)
        .expect("seed Relationship");
    core.close().expect("close canonical fixture");
    fs::remove_dir_all(
        library
            .path()
            .join(GENESIS_METADATA_DIRECTORY)
            .join("recovery"),
    )
    .expect("remove isolated recovery material");
    (library, source.id)
}

fn connection(library: &Path) -> Connection {
    Connection::open(
        library
            .join(GENESIS_METADATA_DIRECTORY)
            .join(RECORD_STORE_FILENAME),
    )
    .expect("open closed fixture Store")
}

#[test]
fn missing_noncritical_creation_provenance_is_diagnosed_without_hiding_facts() {
    let (library, source) = fixture();
    connection(library.path())
        .execute("ALTER TABLE store_meta DROP COLUMN created_at", [])
        .expect("remove only noncritical creation provenance");
    let reopened = CoreState::open(library.path()).expect("interpret surviving canonical facts");
    assert!(reopened
        .status()
        .expect("diagnostic status")
        .record_store
        .degraded_reason
        .as_deref()
        .is_some_and(|reason| reason.contains("created_at")));
    assert_eq!(
        reopened
            .get_property(source, "fact")
            .expect("surviving Property"),
        Some(PropertyValue::Integer(7))
    );
    let columns: i64 = connection(library.path())
        .query_row(
            "SELECT COUNT(*) FROM pragma_table_info('store_meta') WHERE name='created_at'",
            [],
            |row| row.get(0),
        )
        .expect("missing provenance was not fabricated");
    assert_eq!(columns, 0);
}

#[test]
fn partial_v3_to_v4_schema_is_rejected_without_rewriting_canonical_rows() {
    let (library, _) = fixture();
    let store = connection(library.path());
    store
        .pragma_update(None, "user_version", 3)
        .expect("claim legacy schema despite installed ledger");
    store
        .execute("UPDATE store_meta SET schema_version=3", [])
        .expect("set legacy metadata");
    drop(store);
    assert!(CoreState::open(library.path()).is_err());
    let reopened = connection(library.path());
    let schema_version: i64 = reopened
        .pragma_query_value(None, "user_version", |row| row.get(0))
        .expect("unchanged schema version");
    assert_eq!(schema_version, 3);
    let nodes: i64 = reopened
        .query_row("SELECT COUNT(*) FROM nodes", [], |row| row.get(0))
        .expect("existing Nodes remain");
    assert_eq!(nodes, 2);
}

#[test]
fn short_node_uuid_blob_is_rejected() {
    let (library, id) = fixture();
    let connection = connection(library.path());
    connection
        .execute_batch("PRAGMA ignore_check_constraints=ON;")
        .expect("construct isolated malformed fixture");
    connection
        .execute(
            "UPDATE nodes SET node_uuid=x'010203' WHERE node_uuid=?1",
            [id.as_bytes().as_slice()],
        )
        .expect("inject malformed Node UUID");
    drop(connection);
    assert!(CoreState::open(library.path()).is_err());
}

#[test]
fn invalid_lifecycle_value_is_rejected() {
    let (library, id) = fixture();
    let connection = connection(library.path());
    connection
        .execute_batch("PRAGMA ignore_check_constraints=ON;")
        .expect("construct isolated malformed fixture");
    connection
        .execute(
            "UPDATE nodes SET lifecycle_state='archived' WHERE node_uuid=?1",
            [id.as_bytes().as_slice()],
        )
        .expect("inject invalid lifecycle");
    drop(connection);
    assert!(CoreState::open(library.path()).is_err());
}

#[test]
fn missing_issued_id_for_existing_node_is_rejected() {
    let (library, id) = fixture();
    let raw_store = connection(library.path());
    raw_store
        .execute_batch("PRAGMA foreign_keys=OFF;")
        .expect("construct incomplete issued-ID fixture");
    raw_store
        .execute(
            "DELETE FROM issued_node_ids WHERE node_uuid=?1",
            [id.as_bytes().as_slice()],
        )
        .expect("inject incomplete issued-ID authority");
    drop(raw_store);
    assert!(CoreState::open(library.path()).is_err());
}

#[test]
fn property_rowid_without_reconcilable_identity_is_reported_and_preserved() {
    let (library, _) = fixture();
    let raw_store = connection(library.path());
    raw_store
        .execute_batch("PRAGMA foreign_keys=OFF;")
        .expect("construct isolated dangling-row fixture");
    raw_store
        .execute("UPDATE node_properties SET node_rowid=999999", [])
        .expect("orphan canonical Property row");
    drop(raw_store);
    let error = CoreState::open(library.path())
        .err()
        .expect("canonical inconsistency");
    assert!(error.to_string().contains("canonical inconsistency"));
    let remaining: i64 = connection(library.path())
        .query_row(
            "SELECT COUNT(*) FROM node_properties WHERE node_rowid=999999",
            [],
            |row| row.get(0),
        )
        .expect("unresolved evidence remains on disk");
    assert_eq!(remaining, 1);
}

#[test]
fn relationship_endpoint_without_issued_identity_is_reported_not_deleted() {
    let (library, _) = fixture();
    let raw_store = connection(library.path());
    let unissued = NodeId::new();
    raw_store
        .execute_batch("PRAGMA foreign_keys=OFF;")
        .expect("construct isolated unsupported-identity fixture");
    raw_store
        .execute(
            "UPDATE relationships SET target_uuid=?1",
            [unissued.as_bytes().as_slice()],
        )
        .expect("refer to genuinely unissued identity");
    drop(raw_store);
    let error = CoreState::open(library.path())
        .err()
        .expect("canonical inconsistency");
    assert!(error.to_string().contains("canonical inconsistency"));
    let remaining: i64 = connection(library.path())
        .query_row("SELECT COUNT(*) FROM relationships", [], |row| row.get(0))
        .expect("surviving Relationship was not silently deleted");
    assert_eq!(remaining, 1);
}

#[test]
fn revision_behind_committed_history_is_derived_without_rewriting_history() {
    let (library, source) = fixture();
    let original = connection(library.path());
    let revision: i64 = original
        .query_row("SELECT current_revision FROM store_meta", [], |row| {
            row.get(0)
        })
        .expect("committed revision");
    let changes: i64 = original
        .query_row("SELECT COUNT(*) FROM record_changes", [], |row| row.get(0))
        .expect("surviving history");
    original
        .execute("UPDATE store_meta SET current_revision=0", [])
        .expect("rewind only derived metadata");
    drop(original);
    let recovered =
        CoreState::open(library.path()).expect("derive revision from surviving history");
    assert_eq!(
        recovered
            .status()
            .expect("repaired revision")
            .index_sync
            .record_revision,
        revision as u64
    );
    assert_eq!(
        recovered
            .get_property(source, "fact")
            .expect("canonical fact"),
        Some(PropertyValue::Integer(7))
    );
    let unchanged: i64 = connection(library.path())
        .query_row("SELECT COUNT(*) FROM record_changes", [], |row| row.get(0))
        .expect("history was not rewritten");
    assert_eq!(unchanged, changes);
}

#[test]
fn unprovable_revision_with_missing_history_is_reported_not_guessed() {
    let (library, _) = fixture();
    connection(library.path())
        .execute_batch("DELETE FROM record_changes; UPDATE store_meta SET current_revision=0;")
        .expect("remove only surviving journal proof");
    let error = CoreState::open(library.path())
        .err()
        .expect("cannot infer a unique revision");
    assert!(error.to_string().contains("canonical inconsistency"));
}
