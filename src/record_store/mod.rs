mod migrations;
mod operations;
mod projection;
mod recovery;
mod schema;
mod stable_scan;

pub use stable_scan::StableReadSnapshot;

pub(crate) use projection::{
    JournalDelta, ProjectionNode, ProjectionPatch, ProjectionPropertyDefinition,
    ProjectionRelationship, ProjectionRelationshipDefinition, ProjectionSnapshot,
};

use std::{path::PathBuf, time::Duration};

use rusqlite::Connection;

use super::{CoreError, StoreStatus};

pub(crate) use schema::RECORD_SCHEMA_VERSION;

const STORE_NAME: &str = "Record Store";

pub(crate) struct RecordStore {
    connection: Connection,
    path: PathBuf,
}

impl RecordStore {
    pub(crate) fn open(path: PathBuf) -> Result<Self, CoreError> {
        let mut connection = Connection::open(&path)?;
        configure_connection(&connection)?;
        migrations::migrate(&mut connection)?;
        validate(&connection)?;
        reconcile_revision(&connection)?;

        Ok(Self { connection, path })
    }

    pub(crate) fn status(&self) -> Result<StoreStatus, CoreError> {
        Ok(StoreStatus {
            path: self.path.clone(),
            schema_version: schema_version(&self.connection)?,
            integrity_ok: quick_check(&self.connection)?,
            degraded_reason: if has_created_at(&self.connection)? {
                None
            } else {
                Some("Store creation provenance unavailable: created_at column is missing".into())
            },
        })
    }

    pub(crate) fn close(self) -> Result<(), CoreError> {
        self.connection
            .close()
            .map_err(|(_, source)| CoreError::CloseStore {
                store: STORE_NAME,
                source,
            })
    }

    #[cfg(test)]
    pub(crate) fn is_id_issued(&self, id: crate::NodeId) -> Result<bool, CoreError> {
        Ok(self.connection.query_row(
            "SELECT EXISTS(SELECT 1 FROM issued_node_ids WHERE node_uuid=?1)",
            [id.as_bytes().as_slice()],
            |row| row.get(0),
        )?)
    }
}

fn configure_connection(connection: &Connection) -> Result<(), CoreError> {
    connection.busy_timeout(Duration::from_secs(5))?;
    connection.pragma_update(None, "foreign_keys", true)?;

    // Canonical durability favors WAL + FULL. SQLite reports the actual mode;
    // unsupported filesystems fall back explicitly to the rollback journal.
    let journal_mode: String =
        connection.pragma_query_value(None, "journal_mode", |row| row.get(0))?;
    if !journal_mode.eq_ignore_ascii_case("wal") {
        let requested: String =
            connection.query_row("PRAGMA journal_mode = WAL", [], |row| row.get(0))?;
        if !requested.eq_ignore_ascii_case("wal") {
            connection.execute_batch("PRAGMA journal_mode = DELETE;")?;
        }
    }
    connection.execute_batch("PRAGMA synchronous = FULL;")?;
    Ok(())
}

fn schema_version(connection: &Connection) -> Result<u32, CoreError> {
    connection
        .pragma_query_value(None, "user_version", |row| row.get(0))
        .map_err(CoreError::from)
}

fn quick_check(connection: &Connection) -> Result<bool, CoreError> {
    let result: String = connection.query_row("PRAGMA quick_check", [], |row| row.get(0))?;
    Ok(result == "ok")
}

fn has_created_at(connection: &Connection) -> Result<bool, CoreError> {
    Ok(connection.query_row(
        "SELECT EXISTS(SELECT 1 FROM pragma_table_info('store_meta') WHERE name='created_at')",
        [],
        |row| row.get(0),
    )?)
}

fn validate(connection: &Connection) -> Result<(), CoreError> {
    if !quick_check(connection)? {
        return Err(CoreError::CorruptStore {
            store: STORE_NAME,
            reason: "SQLite quick_check did not return ok".into(),
        });
    }

    let broken_references: i64 =
        connection.query_row("SELECT COUNT(*) FROM pragma_foreign_key_check", [], |row| {
            row.get(0)
        })?;
    if broken_references != 0 {
        return Err(CoreError::CorruptStore {
             store: STORE_NAME,
            reason: "canonical inconsistency: a surviving fact references identity history that cannot be reconciled".into(),
        });
    }

    let user_version = schema_version(connection)?;
    if user_version != RECORD_SCHEMA_VERSION {
        return Err(CoreError::UnsupportedSchema {
            store: STORE_NAME,
            found: user_version,
            supported: RECORD_SCHEMA_VERSION,
        });
    }

    let (row_count, meta_version, minimum_reader, current_revision): (u32, u32, u32, i64) =
        connection.query_row(
            "SELECT COUNT(*), MIN(schema_version), MIN(minimum_reader_version), MIN(current_revision) FROM store_meta",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )?;

    if row_count != 1
        || meta_version != RECORD_SCHEMA_VERSION
        || minimum_reader > RECORD_SCHEMA_VERSION
        || current_revision < 0
    {
        return Err(CoreError::CorruptStore {
            store: STORE_NAME,
            reason: "store_meta is missing or inconsistent with the supported schema".into(),
        });
    }

    let unissued_nodes: u32 = connection.query_row(
        "SELECT COUNT(*) FROM nodes n \
         LEFT JOIN issued_node_ids i ON i.node_uuid=n.node_uuid \
         WHERE i.node_uuid IS NULL",
        [],
        |row| row.get(0),
    )?;
    if unissued_nodes != 0 {
        return Err(CoreError::CorruptStore {
            store: STORE_NAME,
            reason: "one or more Node Records lack an Issued-ID Ledger entry".into(),
        });
    }

    Ok(())
}

/// `current_revision` is bookkeeping, not a source of canonical events. Only
/// the unbroken surviving journal tail can establish a unique later revision.
fn reconcile_revision(connection: &Connection) -> Result<(), CoreError> {
    let current: i64 = connection.query_row(
        "SELECT current_revision FROM store_meta WHERE singleton=1",
        [],
        |row| row.get(0),
    )?;
    let latest_fact: i64 = connection.query_row(
        "SELECT COALESCE(MAX(revision), 0) FROM (
            SELECT MAX(modified_revision) AS revision FROM nodes UNION ALL
            SELECT MAX(modified_revision) FROM node_properties UNION ALL
            SELECT MAX(modified_revision) FROM relationships UNION ALL
            SELECT MAX(modified_revision) FROM property_definitions UNION ALL
            SELECT MAX(modified_revision) FROM relationship_definitions UNION ALL
            SELECT MAX(revision) FROM record_changes
         )",
        [],
        |row| row.get(0),
    )?;
    if current == latest_fact {
        return Ok(());
    }
    let (first, last, count): (Option<i64>, Option<i64>, i64) = connection.query_row(
        "SELECT MIN(revision), MAX(revision), COUNT(DISTINCT revision) FROM record_changes",
        [],
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
    )?;
    if current < latest_fact
        && last == Some(latest_fact)
        && first.is_some_and(|first| count == latest_fact - first + 1)
    {
        connection.execute(
            "UPDATE store_meta SET current_revision=?1 WHERE singleton=1 AND current_revision=?2",
            [latest_fact, current],
        )?;
        return Ok(());
    }
    // No lower authoritative bound can prove a later, already-pruned commit.
    // An apparently higher metadata value is likewise not invented history.
    if latest_fact > current || last.is_some() {
        return Err(CoreError::CorruptStore {
            store: STORE_NAME,
            reason: "canonical inconsistency: committed Record revision cannot be uniquely derived from surviving history".into(),
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use rusqlite::Connection;

    use super::*;
    use crate::NodeId;

    fn schema_fingerprint(connection: &Connection) -> Vec<(String, String, String)> {
        let mut statement = connection
            .prepare(
                "SELECT type, name, sql FROM sqlite_schema \
                 WHERE name NOT LIKE 'sqlite_%' ORDER BY type, name",
            )
            .expect("prepare schema query");
        statement
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))
            .expect("query schema")
            .collect::<Result<Vec<_>, _>>()
            .expect("collect schema")
    }

    #[test]
    fn empty_to_current_migration_is_deterministic() {
        let one = tempfile::NamedTempFile::new().expect("first DB");
        let two = tempfile::NamedTempFile::new().expect("second DB");
        let first = RecordStore::open(one.path().to_path_buf()).expect("first store");
        let second = RecordStore::open(two.path().to_path_buf()).expect("second store");

        assert_eq!(
            schema_fingerprint(&first.connection),
            schema_fingerprint(&second.connection)
        );
    }

    #[test]
    fn unsupported_schema_version_fails_without_rewriting_it() {
        let file = tempfile::NamedTempFile::new().expect("DB");
        let connection = Connection::open(file.path()).expect("open raw DB");
        connection
            .pragma_update(None, "user_version", RECORD_SCHEMA_VERSION + 1)
            .expect("set future schema");
        drop(connection);

        assert!(matches!(
            RecordStore::open(file.path().to_path_buf()),
            Err(CoreError::UnsupportedSchema { .. })
        ));
        let reopened = Connection::open(file.path()).expect("reopen raw DB");
        assert_eq!(
            reopened
                .pragma_query_value(None, "user_version", |row| row.get::<_, u32>(0))
                .expect("read schema"),
            RECORD_SCHEMA_VERSION + 1
        );
    }

    #[test]
    fn legacy_schema_adoption_preserves_unowned_tables() {
        let file = tempfile::NamedTempFile::new().expect("DB");
        let mut connection = Connection::open(file.path()).expect("open raw DB");
        let transaction = connection.transaction().expect("transaction");
        transaction
            .execute_batch(schema::RECORD_SCHEMA_V1)
            .expect("install v1 schema");
        transaction
            .execute(
                "INSERT INTO store_meta (singleton, schema_version, current_revision, minimum_reader_version, created_at)                  VALUES (1, 2, 0, 1, '2026-09-07T00:00:00Z')",
                [],
            )
            .expect("insert v2 metadata");
        transaction
            .pragma_update(None, "user_version", 2)
            .expect("set v2 schema");
        transaction.commit().expect("commit v2 fixture");
        connection
            .execute("CREATE TABLE legacy_capability_data (value TEXT)", [])
            .expect("install unowned legacy table");
        connection
            .execute(
                "INSERT INTO legacy_capability_data VALUES ('preserved')",
                [],
            )
            .expect("seed unowned table");
        drop(connection);

        RecordStore::open(file.path().to_path_buf()).expect("adopt legacy version");
        let reopened = Connection::open(file.path()).expect("reopen DB");
        assert_eq!(
            reopened
                .pragma_query_value(None, "user_version", |row| row.get::<_, u32>(0))
                .expect("read user_version"),
            RECORD_SCHEMA_VERSION
        );
        let sentinel: String = reopened
            .query_row("SELECT value FROM legacy_capability_data", [], |row| {
                row.get(0)
            })
            .expect("read preserved table");
        assert_eq!(sentinel, "preserved");
    }

    #[test]
    fn v4_to_v5_preserves_relationships_and_issued_endpoints() {
        let file = tempfile::NamedTempFile::new().expect("DB");
        let mut connection = Connection::open(file.path()).expect("open raw DB");
        let source = NodeId::new();
        let target = NodeId::new();
        let transaction = connection.transaction().expect("transaction");
        transaction
            .execute_batch(schema::RECORD_SCHEMA_V1)
            .expect("install v1 schema");
        transaction
            .execute_batch(schema::RECORD_SCHEMA_V4)
            .expect("install v4 issued-ID ledger");
        transaction
            .execute(
                "INSERT INTO store_meta (singleton, schema_version, current_revision, minimum_reader_version, created_at) \
                 VALUES (1, 4, 1, 1, '2026-09-14T00:00:00Z')",
                [],
            )
            .expect("store metadata");
        for id in [source, target] {
            transaction
                .execute(
                    "INSERT INTO issued_node_ids (node_uuid) VALUES (?1)",
                    [id.as_bytes().as_slice()],
                )
                .expect("record issued identity");
            transaction
                .execute(
                    "INSERT INTO nodes (node_uuid, lifecycle_state, created_revision, modified_revision) \
                     VALUES (?1, 'active', 1, 1)",
                    [id.as_bytes().as_slice()],
                )
                .expect("node");
        }
        transaction
            .execute(
                "INSERT INTO relationships (source_node_rowid, relationship_key, target_node_rowid, created_revision, modified_revision) \
                 SELECT source.node_rowid, 'edge', target.node_rowid, 1, 1 \
                 FROM nodes source, nodes target WHERE source.node_uuid=?1 AND target.node_uuid=?2",
                rusqlite::params![source.as_bytes().as_slice(), target.as_bytes().as_slice()],
            )
            .expect("old rowid relationship");
        transaction
            .pragma_update(None, "user_version", 4)
            .expect("set v4 version");
        transaction.commit().expect("commit v4 fixture");
        drop(connection);

        let upgraded = RecordStore::open(file.path().to_path_buf()).expect("migrate v4 fixture");
        let (source_bytes, target_bytes): (Vec<u8>, Vec<u8>) = upgraded
            .connection
            .query_row(
                "SELECT source_uuid, target_uuid FROM relationships WHERE relationship_key='edge'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .expect("surviving relationship");
        assert_eq!(source_bytes.as_slice(), source.as_bytes().as_slice());
        assert_eq!(target_bytes.as_slice(), target.as_bytes().as_slice());
        assert_eq!(
            upgraded.status().expect("status").schema_version,
            RECORD_SCHEMA_VERSION
        );
    }

    #[test]
    fn malformed_metadata_fails_safely() {
        let file = tempfile::NamedTempFile::new().expect("DB");
        let store = RecordStore::open(file.path().to_path_buf()).expect("initialize store");
        store
            .connection
            .execute(
                "UPDATE store_meta SET schema_version = ?1",
                [RECORD_SCHEMA_VERSION + 1],
            )
            .expect("corrupt metadata");
        drop(store);

        assert!(matches!(
            RecordStore::open(file.path().to_path_buf()),
            Err(CoreError::CorruptStore { .. })
        ));
    }

    #[test]
    fn node_schema_contains_no_path_or_semantic_identity_columns() {
        let file = tempfile::NamedTempFile::new().expect("DB");
        let store = RecordStore::open(file.path().to_path_buf()).expect("initialize store");
        let mut statement = store
            .connection
            .prepare("PRAGMA table_info(nodes)")
            .expect("table info");
        let columns = statement
            .query_map([], |row| row.get::<_, String>(1))
            .expect("query columns")
            .collect::<Result<Vec<_>, _>>()
            .expect("collect columns");

        assert_eq!(
            columns,
            [
                "node_rowid",
                "node_uuid",
                "lifecycle_state",
                "created_revision",
                "modified_revision"
            ]
        );
        for retired_identity in ["path", "relative_path", "filename", "title", "type"] {
            assert!(!columns.iter().any(|column| column == retired_identity));
        }
    }

    #[test]
    fn core_schema_contains_only_generic_storage_areas() {
        let file = tempfile::NamedTempFile::new().expect("DB");
        let store = RecordStore::open(file.path().to_path_buf()).expect("initialize store");
        let mut statement = store
            .connection
            .prepare(
                "SELECT name FROM sqlite_schema WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name",
            )
            .expect("prepare tables query");
        let tables = statement
            .query_map([], |row| row.get::<_, String>(0))
            .expect("query tables")
            .collect::<Result<Vec<_>, _>>()
            .expect("collect tables");
        assert_eq!(
            tables,
            [
                "issued_node_ids",
                "node_properties",
                "nodes",
                "property_definitions",
                "record_changes",
                "relationship_definitions",
                "relationships",
                "store_meta",
            ]
        );
        for semantic_table in ["projects", "worlds", "roadmaps", "pages", "files"] {
            assert!(!tables.iter().any(|table| table == semantic_table));
        }
    }

    #[test]
    fn phase_four_v1_schema_migrates_without_rewriting_existing_truth() {
        let file = tempfile::NamedTempFile::new().expect("DB");
        let mut connection = Connection::open(file.path()).expect("open raw DB");
        let transaction = connection.transaction().expect("transaction");
        transaction
            .execute_batch(schema::RECORD_SCHEMA_V1)
            .expect("install v1 schema");
        transaction
            .execute(
                "INSERT INTO store_meta (singleton, schema_version, current_revision, minimum_reader_version, created_at) \
                 VALUES (1, 1, 0, 1, '2026-09-04T00:00:00Z')",
                [],
            )
            .expect("insert v1 metadata");
        transaction
            .pragma_update(None, "user_version", 1)
            .expect("set v1 schema");
        transaction.commit().expect("commit v1 fixture");
        drop(connection);

        let store = RecordStore::open(file.path().to_path_buf()).expect("migrate store");
        assert_eq!(
            store.status().expect("status").schema_version,
            RECORD_SCHEMA_VERSION
        );
        let capability_tables: u32 = store
            .connection
            .query_row(
                "SELECT COUNT(*) FROM sqlite_schema WHERE type='table' AND name LIKE 'cap_%'",
                [],
                |row| row.get(0),
            )
            .expect("query capability tables");
        assert_eq!(capability_tables, 0);
    }

    #[test]
    fn phase_five_v2_schema_migrates_without_rewriting_existing_truth() {
        let file = tempfile::NamedTempFile::new().expect("DB");
        let mut connection = Connection::open(file.path()).expect("open raw DB");
        let transaction = connection.transaction().expect("transaction");
        transaction
            .execute_batch(schema::RECORD_SCHEMA_V1)
            .expect("install v1 schema");
        transaction
            .execute(
                "INSERT INTO store_meta (singleton, schema_version, current_revision, minimum_reader_version, created_at) \
                 VALUES (1, 2, 0, 1, '2026-09-05T00:00:00Z')",
                [],
            )
            .expect("insert v2 metadata");
        transaction
            .pragma_update(None, "user_version", 2)
            .expect("set v2 schema");
        transaction.commit().expect("commit v2 fixture");
        drop(connection);

        let store = RecordStore::open(file.path().to_path_buf()).expect("migrate store");
        assert_eq!(
            store.status().expect("status").schema_version,
            RECORD_SCHEMA_VERSION
        );
        let capability_tables: u32 = store
            .connection
            .query_row(
                "SELECT COUNT(*) FROM sqlite_schema WHERE type='table' AND name LIKE 'cap_%'",
                [],
                |row| row.get(0),
            )
            .expect("query capability tables");
        assert_eq!(capability_tables, 0);
    }

    #[test]
    fn v3_schema_migrates_transactionally_and_preserves_canonical_truth() {
        let file = tempfile::NamedTempFile::new().expect("DB");
        let (active, tombstoned) = {
            let mut store = RecordStore::open(file.path().to_path_buf()).expect("seed store");
            let active = store.create_node().expect("active Node");
            let tombstoned = store.create_node().expect("tombstoned Node");
            store
                .apply_batch(&[
                    crate::Mutation::SetProperty {
                        node_id: active.id,
                        key: "title".into(),
                        value: crate::PropertyValue::Text("preserved".into()),
                    },
                    crate::Mutation::AddRelationship {
                        source: active.id,
                        key: "links".into(),
                        target: tombstoned.id,
                    },
                ])
                .expect("seed facts");
            store
                .apply_batch(&[crate::Mutation::TombstoneNode {
                    node_id: tombstoned.id,
                }])
                .expect("tombstone Node");
            store.close().expect("close fixture");
            (active, tombstoned)
        };
        let connection = Connection::open(file.path()).expect("open raw DB");
        connection
            .execute_batch(
                "PRAGMA foreign_keys=OFF;
                 BEGIN IMMEDIATE;
                 ALTER TABLE relationships RENAME TO relationships_v5;
                 CREATE TABLE relationships (
                     relationship_rowid INTEGER PRIMARY KEY,
                     source_node_rowid INTEGER NOT NULL REFERENCES nodes(node_rowid) ON DELETE CASCADE,
                     relationship_key TEXT NOT NULL CHECK (length(relationship_key) > 0),
                     target_node_rowid INTEGER NOT NULL REFERENCES nodes(node_rowid) ON DELETE CASCADE,
                     created_revision INTEGER NOT NULL CHECK (created_revision > 0),
                     modified_revision INTEGER NOT NULL CHECK (modified_revision >= created_revision),
                     UNIQUE (source_node_rowid, relationship_key, target_node_rowid)
                 );
                 INSERT INTO relationships
                     (relationship_rowid, source_node_rowid, relationship_key, target_node_rowid,
                      created_revision, modified_revision)
                 SELECT r.relationship_rowid, source.node_rowid, r.relationship_key,
                        target.node_rowid, r.created_revision, r.modified_revision
                 FROM relationships_v5 r
                 JOIN nodes source ON source.node_uuid=r.source_uuid
                 JOIN nodes target ON target.node_uuid=r.target_uuid;
                 DROP TABLE relationships_v5;
                 CREATE INDEX relationships_target_idx
                     ON relationships(target_node_rowid, relationship_key);
                 DROP TABLE issued_node_ids;
                 COMMIT;
                 PRAGMA foreign_keys=ON;",
            )
            .expect("construct faithful pre-ledger rowid schema without losing seeded facts");
        connection
            .execute(
                "UPDATE store_meta SET schema_version=3, minimum_reader_version=1 WHERE singleton=1",
                [],
            )
            .expect("set v3 metadata");
        connection
            .pragma_update(None, "user_version", 3)
            .expect("set v3 schema");
        connection
            .execute_batch("PRAGMA wal_checkpoint(TRUNCATE); PRAGMA journal_mode=DELETE;")
            .expect("consolidate fixture");
        drop(connection);

        let store = RecordStore::open(file.path().to_path_buf()).expect("migrate v3 store");
        assert_eq!(
            store.status().expect("status").schema_version,
            RECORD_SCHEMA_VERSION
        );
        assert_eq!(store.get_node(active.id).expect("active Node"), active);
        assert_eq!(
            store
                .get_node(tombstoned.id)
                .expect("tombstoned Node")
                .lifecycle_state,
            crate::LifecycleState::Tombstoned
        );
        assert_eq!(
            store.get_property(active.id, "title").expect("Property"),
            Some(crate::PropertyValue::Text("preserved".into()))
        );
        assert_eq!(
            store
                .query_relationships(
                    active.id,
                    crate::RelationshipDirection::Outgoing,
                    Some("links")
                )
                .expect("Relationship")
                .len(),
            1
        );
        assert!(store.is_id_issued(active.id).expect("active issuance"));
        assert!(store
            .is_id_issued(tombstoned.id)
            .expect("tombstoned issuance"));
    }

    #[test]
    fn inconsistent_v3_metadata_fails_without_partial_migration() {
        let file = tempfile::NamedTempFile::new().expect("DB");
        let node_id = NodeId::new();
        let mut connection = Connection::open(file.path()).expect("open raw DB");
        let transaction = connection.transaction().expect("transaction");
        transaction
            .execute_batch(schema::RECORD_SCHEMA_V1)
            .expect("install v3 physical schema");
        transaction
            .execute(
                "INSERT INTO store_meta (singleton, schema_version, current_revision, minimum_reader_version, created_at) \
                 VALUES (1, 2, 1, 1, '2026-09-11T00:00:00Z')",
                [],
            )
            .expect("insert inconsistent metadata");
        transaction
            .execute(
                "INSERT INTO nodes (node_uuid, lifecycle_state, created_revision, modified_revision) \
                 VALUES (?1, 'active', 1, 1)",
                [node_id.as_bytes().as_slice()],
            )
            .expect("seed canonical Node");
        transaction
            .pragma_update(None, "user_version", 3)
            .expect("set v3 schema");
        transaction.commit().expect("commit malformed fixture");
        drop(connection);

        assert!(matches!(
            RecordStore::open(file.path().to_path_buf()),
            Err(CoreError::CorruptStore { .. })
        ));
        let reopened = Connection::open(file.path()).expect("reopen raw DB");
        assert_eq!(
            reopened
                .pragma_query_value(None, "user_version", |row| row.get::<_, u32>(0))
                .expect("read schema"),
            3
        );
        let ledger_table: u32 = reopened
            .query_row(
                "SELECT COUNT(*) FROM sqlite_schema WHERE type='table' AND name='issued_node_ids'",
                [],
                |row| row.get(0),
            )
            .expect("query Issued-ID Ledger");
        assert_eq!(ledger_table, 0);
        let node_count: u32 = reopened
            .query_row("SELECT COUNT(*) FROM nodes", [], |row| row.get(0))
            .expect("query preserved Nodes");
        assert_eq!(node_count, 1);
        let preserved_id: Vec<u8> = reopened
            .query_row("SELECT node_uuid FROM nodes", [], |row| row.get(0))
            .expect("read preserved Node ID");
        assert_eq!(preserved_id, node_id.as_bytes().to_vec());
    }

    #[test]
    fn failed_single_node_creation_does_not_consume_identity() {
        let file = tempfile::NamedTempFile::new().expect("DB");
        let mut store = RecordStore::open(file.path().to_path_buf()).expect("store");
        let node_id = NodeId::new();
        store
            .connection
            .execute_batch(
                "CREATE TRIGGER fail_node BEFORE INSERT ON nodes \
                 BEGIN SELECT RAISE(ABORT, 'controlled creation failure'); END;",
            )
            .expect("install controlled failure");
        assert!(store.create_node_with_id(node_id).is_err());
        assert!(!store.is_id_issued(node_id).expect("unconsumed ID"));
        assert_eq!(store.current_revision().expect("revision"), 0);
        store
            .connection
            .execute("DROP TRIGGER fail_node", [])
            .expect("remove controlled failure");
        assert_eq!(
            store
                .create_node_with_id(node_id)
                .expect("retry same identity")
                .id,
            node_id
        );
    }

    #[test]
    fn batch_creation_reserves_every_id_atomically() {
        let success_file = tempfile::NamedTempFile::new().expect("success DB");
        let mut success =
            RecordStore::open(success_file.path().to_path_buf()).expect("success store");
        let nodes = success.create_nodes(4).expect("batch create");
        assert!(nodes
            .iter()
            .all(|node| success.is_id_issued(node.id).expect("issued ID")));
        let issued_count: u32 = success
            .connection
            .query_row("SELECT COUNT(*) FROM issued_node_ids", [], |row| row.get(0))
            .expect("issued count");
        assert_eq!(issued_count, 4);

        let failure_file = tempfile::NamedTempFile::new().expect("failure DB");
        let mut failure =
            RecordStore::open(failure_file.path().to_path_buf()).expect("failure store");
        failure
            .connection
            .execute_batch(
                "CREATE TRIGGER fail_second_node BEFORE INSERT ON nodes \
                 WHEN (SELECT COUNT(*) FROM nodes) >= 1 \
                 BEGIN SELECT RAISE(ABORT, 'controlled batch failure'); END;",
            )
            .expect("install controlled failure");
        assert!(failure.create_nodes(2).is_err());
        let counts: (u32, u32) = failure
            .connection
            .query_row(
                "SELECT (SELECT COUNT(*) FROM issued_node_ids), (SELECT COUNT(*) FROM nodes)",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .expect("post-failure counts");
        assert_eq!(counts, (0, 0));
        assert_eq!(failure.current_revision().expect("revision"), 0);
    }

    #[test]
    fn unversioned_nonempty_database_is_not_claimed_as_genesis() {
        let file = tempfile::NamedTempFile::new().expect("DB");
        let connection = Connection::open(file.path()).expect("open raw DB");
        connection
            .execute("CREATE TABLE foreign_data(value TEXT)", [])
            .expect("create foreign table");
        drop(connection);

        assert!(matches!(
            RecordStore::open(Path::new(file.path()).to_path_buf()),
            Err(CoreError::CorruptStore { .. })
        ));
    }

    #[test]
    fn canonical_store_uses_explicit_durability_settings() {
        let file = tempfile::NamedTempFile::new().expect("DB");
        let store = RecordStore::open(file.path().to_path_buf()).expect("initialize store");
        let foreign_keys: u32 = store
            .connection
            .pragma_query_value(None, "foreign_keys", |row| row.get(0))
            .expect("foreign_keys");
        let synchronous: u32 = store
            .connection
            .pragma_query_value(None, "synchronous", |row| row.get(0))
            .expect("synchronous");
        let busy_timeout: u32 = store
            .connection
            .pragma_query_value(None, "busy_timeout", |row| row.get(0))
            .expect("busy_timeout");
        let journal_mode: String = store
            .connection
            .pragma_query_value(None, "journal_mode", |row| row.get(0))
            .expect("journal_mode");

        assert_eq!(foreign_keys, 1);
        assert_eq!(synchronous, 2, "FULL durability is required");
        assert_eq!(busy_timeout, 5_000);
        assert!(matches!(journal_mode.as_str(), "wal" | "delete"));
    }
}
