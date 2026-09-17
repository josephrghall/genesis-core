mod migrations;
mod projection;
mod query;
mod schema;

use std::{
    fs,
    path::{Path, PathBuf},
    time::Duration,
};

use rusqlite::Connection;

use super::{CoreError, IndexRebuildReason, ReconcileOutcome, StoreStatus};
use crate::record_store::{JournalDelta, ProjectionSnapshot, RecordStore};

const STORE_NAME: &str = "Index Store";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FailurePoint {
    BeforeApply,
    BeforeCheckpoint,
    DuringRebuild,
    BeforePromotion,
}

pub(crate) struct IndexStore {
    connection: Option<Connection>,
    path: PathBuf,
    failure_point: Option<FailurePoint>,
}

impl IndexStore {
    pub(crate) fn open(path: PathBuf) -> Result<Self, CoreError> {
        let mut connection = Connection::open(&path)?;
        configure_connection(&connection)?;
        migrations::migrate(&mut connection)?;
        validate(&connection)?;
        Ok(Self {
            connection: Some(connection),
            path,
            failure_point: None,
        })
    }

    pub(crate) fn open_for_core(
        path: PathBuf,
        records: &RecordStore,
    ) -> Result<(Self, ReconcileOutcome), CoreError> {
        if !path.exists() {
            let snapshot = records.projection_snapshot()?;
            let store = build_and_promote_without_open_store(&path, &snapshot, false)?;
            return Ok((
                store,
                ReconcileOutcome::Rebuilt {
                    revision: snapshot.revision,
                    reason: IndexRebuildReason::Missing,
                },
            ));
        }
        match Self::open(path.clone()) {
            Ok(mut store) => {
                let outcome = store.reconcile(records)?;
                Ok((store, outcome))
            }
            Err(_) => {
                let snapshot = records.projection_snapshot()?;
                let store = build_and_promote_without_open_store(&path, &snapshot, true)?;
                Ok((
                    store,
                    ReconcileOutcome::Rebuilt {
                        revision: snapshot.revision,
                        reason: IndexRebuildReason::CorruptOrIncompatible,
                    },
                ))
            }
        }
    }

    pub(crate) fn status(&self) -> Result<StoreStatus, CoreError> {
        let connection = self.connection()?;
        Ok(StoreStatus {
            path: self.path.clone(),
            schema_version: schema_version(connection)?,
            integrity_ok: quick_check(connection)?,
            degraded_reason: None,
        })
    }

    pub(crate) fn applied_revision(&self) -> Result<u64, CoreError> {
        let revision: i64 = self.connection()?.query_row(
            "SELECT applied_record_revision FROM index_meta WHERE singleton=1",
            [],
            |row| row.get(0),
        )?;
        Ok(revision as u64)
    }

    pub(crate) fn reconcile(
        &mut self,
        records: &RecordStore,
    ) -> Result<ReconcileOutcome, CoreError> {
        let checkpoint = self.applied_revision()?;
        match records.projection_delta_since(checkpoint)? {
            JournalDelta::Current(revision) => Ok(ReconcileOutcome::Current { revision }),
            JournalDelta::Available(patch) => self.apply_patch(&patch),
            JournalDelta::Unavailable { current_revision } => {
                let outcome = self.rebuild(records, IndexRebuildReason::JournalUnavailable)?;
                debug_assert_eq!(
                    outcome,
                    ReconcileOutcome::Rebuilt {
                        revision: current_revision,
                        reason: IndexRebuildReason::JournalUnavailable,
                    }
                );
                Ok(outcome)
            }
        }
    }

    pub(crate) fn rebuild(
        &mut self,
        records: &RecordStore,
        reason: IndexRebuildReason,
    ) -> Result<ReconcileOutcome, CoreError> {
        self.fail_if(FailurePoint::DuringRebuild)?;
        let snapshot = records.projection_snapshot()?;
        let candidate_path = rebuild_path(&self.path);
        remove_sqlite_family(&candidate_path)?;
        if let Err(error) = build_candidate(&candidate_path, &snapshot) {
            let _ = remove_sqlite_family(&candidate_path);
            return Err(error);
        }
        if let Err(error) = self.fail_if(FailurePoint::BeforePromotion) {
            let _ = remove_sqlite_family(&candidate_path);
            return Err(error);
        }

        self.checkpoint_and_close()?;
        let previous_path = previous_path(&self.path);
        remove_sqlite_family(&previous_path)?;
        remove_auxiliary_files(&self.path)?;
        if let Err(error) = fs::rename(&self.path, &previous_path) {
            self.connection = Some(open_connection(&self.path)?);
            let _ = remove_sqlite_family(&candidate_path);
            return Err(error.into());
        }
        if let Err(error) = fs::rename(&candidate_path, &self.path) {
            let _ = fs::rename(&previous_path, &self.path);
            self.connection = Some(open_connection(&self.path)?);
            let _ = remove_sqlite_family(&candidate_path);
            return Err(error.into());
        }
        match open_connection(&self.path) {
            Ok(connection) => self.connection = Some(connection),
            Err(error) => {
                let _ = remove_sqlite_family(&self.path);
                let _ = fs::rename(&previous_path, &self.path);
                self.connection = Some(open_connection(&self.path)?);
                return Err(error);
            }
        }
        Ok(ReconcileOutcome::Rebuilt {
            revision: snapshot.revision,
            reason,
        })
    }

    pub(crate) fn close(mut self) -> Result<(), CoreError> {
        self.checkpoint_and_close()
    }

    pub(super) fn connection(&self) -> Result<&Connection, CoreError> {
        self.connection.as_ref().ok_or_else(|| {
            CoreError::IndexUnavailable("Index connection is not currently open".into())
        })
    }

    pub(super) fn connection_mut(&mut self) -> Result<&mut Connection, CoreError> {
        self.connection.as_mut().ok_or_else(|| {
            CoreError::IndexUnavailable("Index connection is not currently open".into())
        })
    }

    pub(super) fn fail_if(&mut self, point: FailurePoint) -> Result<(), CoreError> {
        if self.take_failure(point) {
            return Err(CoreError::IndexUnavailable(format!(
                "injected failure at {point:?}"
            )));
        }
        Ok(())
    }

    pub(super) fn take_failure(&mut self, point: FailurePoint) -> bool {
        if self.failure_point == Some(point) {
            self.failure_point = None;
            true
        } else {
            false
        }
    }

    #[cfg(test)]
    pub(crate) fn inject_failure(&mut self, point: FailurePoint) {
        self.failure_point = Some(point);
    }

    #[cfg(test)]
    pub(crate) fn projection_counts(&self) -> Result<(u64, u64, u64), CoreError> {
        let connection = self.connection()?;
        Ok((
            count(connection, "indexed_nodes")?,
            count(connection, "indexed_properties")?,
            count(connection, "indexed_relationships")?,
        ))
    }

    #[cfg(test)]
    pub(crate) fn definition_projection_counts(&self) -> Result<(u64, u64), CoreError> {
        let connection = self.connection()?;
        Ok((
            count(connection, "indexed_property_definitions")?,
            count(connection, "indexed_relationship_definitions")?,
        ))
    }

    fn checkpoint_and_close(&mut self) -> Result<(), CoreError> {
        let Some(connection) = self.connection.take() else {
            return Ok(());
        };
        if let Err(error) = connection.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);") {
            self.connection = Some(connection);
            return Err(error.into());
        }
        match connection.close() {
            Ok(()) => Ok(()),
            Err((connection, source)) => {
                self.connection = Some(connection);
                Err(CoreError::CloseStore {
                    store: STORE_NAME,
                    source,
                })
            }
        }
    }
}

fn build_candidate(path: &Path, snapshot: &ProjectionSnapshot) -> Result<(), CoreError> {
    let mut candidate = IndexStore::open(path.to_path_buf())?;
    candidate.replace_projection(snapshot)?;
    validate_projection(candidate.connection()?, snapshot)?;
    candidate.close()
}

fn build_and_promote_without_open_store(
    path: &Path,
    snapshot: &ProjectionSnapshot,
    preserve_invalid: bool,
) -> Result<IndexStore, CoreError> {
    let candidate_path = rebuild_path(path);
    remove_sqlite_family(&candidate_path)?;
    if let Err(error) = build_candidate(&candidate_path, snapshot) {
        let _ = remove_sqlite_family(&candidate_path);
        return Err(error);
    }
    if path.exists() {
        remove_auxiliary_files(path)?;
        let displaced = if preserve_invalid {
            invalid_path(path)
        } else {
            previous_path(path)
        };
        remove_sqlite_family(&displaced)?;
        fs::rename(path, &displaced)?;
        if let Err(error) = fs::rename(&candidate_path, path) {
            let _ = fs::rename(&displaced, path);
            let _ = remove_sqlite_family(&candidate_path);
            return Err(error.into());
        }
    } else {
        fs::rename(&candidate_path, path)?;
    }
    IndexStore::open(path.to_path_buf())
}

fn validate_projection(
    connection: &Connection,
    snapshot: &ProjectionSnapshot,
) -> Result<(), CoreError> {
    validate(connection)?;
    let foreign_key_failures: i64 =
        connection.query_row("SELECT COUNT(*) FROM pragma_foreign_key_check", [], |row| {
            row.get(0)
        })?;
    let expected_properties = snapshot
        .nodes
        .iter()
        .map(|node| node.properties.len() as u64)
        .sum::<u64>();
    let counts = (
        count(connection, "indexed_nodes")?,
        count(connection, "indexed_properties")?,
        count(connection, "indexed_relationships")?,
        count(connection, "indexed_property_definitions")?,
        count(connection, "indexed_relationship_definitions")?,
    );
    let checkpoint: i64 = connection.query_row(
        "SELECT applied_record_revision FROM index_meta WHERE singleton=1",
        [],
        |row| row.get(0),
    )?;
    if foreign_key_failures != 0
        || counts
            != (
                snapshot.nodes.len() as u64,
                expected_properties,
                snapshot.relationships.len() as u64,
                snapshot.property_definitions.len() as u64,
                snapshot.relationship_definitions.len() as u64,
            )
        || checkpoint as u64 != snapshot.revision
    {
        return Err(CoreError::CorruptStore {
            store: STORE_NAME,
            reason: "rebuilt projection failed canonical validation".into(),
        });
    }
    Ok(())
}

fn count(connection: &Connection, table: &str) -> Result<u64, CoreError> {
    let sql = format!("SELECT COUNT(*) FROM {table}");
    let value: i64 = connection.query_row(&sql, [], |row| row.get(0))?;
    Ok(value as u64)
}

fn open_connection(path: &Path) -> Result<Connection, CoreError> {
    let mut connection = Connection::open(path)?;
    configure_connection(&connection)?;
    migrations::migrate(&mut connection)?;
    validate(&connection)?;
    Ok(connection)
}

fn configure_connection(connection: &Connection) -> Result<(), CoreError> {
    connection.busy_timeout(Duration::from_secs(5))?;
    connection.pragma_update(None, "foreign_keys", true)?;
    let requested: String =
        connection.query_row("PRAGMA journal_mode = WAL", [], |row| row.get(0))?;
    if !requested.eq_ignore_ascii_case("wal") {
        connection.execute_batch("PRAGMA journal_mode = DELETE;")?;
    }
    connection.execute_batch("PRAGMA synchronous = NORMAL;")?;
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

fn validate(connection: &Connection) -> Result<(), CoreError> {
    if !quick_check(connection)? {
        return Err(CoreError::CorruptStore {
            store: STORE_NAME,
            reason: "SQLite quick_check did not return ok".into(),
        });
    }
    let user_version = schema_version(connection)?;
    if user_version != schema::INDEX_SCHEMA_VERSION {
        return Err(CoreError::UnsupportedSchema {
            store: STORE_NAME,
            found: user_version,
            supported: schema::INDEX_SCHEMA_VERSION,
        });
    }
    let (row_count, meta_version, minimum_reader, applied_revision): (u32, u32, u32, i64) =
        connection.query_row(
            "SELECT COUNT(*), MIN(schema_version), MIN(minimum_reader_version), MIN(applied_record_revision) FROM index_meta",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )?;
    if row_count != 1
        || meta_version != schema::INDEX_SCHEMA_VERSION
        || minimum_reader > schema::INDEX_SCHEMA_VERSION
        || applied_revision < 0
    {
        return Err(CoreError::CorruptStore {
            store: STORE_NAME,
            reason: "index_meta is missing or inconsistent with the supported schema".into(),
        });
    }
    Ok(())
}

fn rebuild_path(path: &Path) -> PathBuf {
    sibling_with_role(path, &format!("rebuild-{}", uuid::Uuid::now_v7()))
}

fn previous_path(path: &Path) -> PathBuf {
    sibling_with_role(path, "previous")
}

fn invalid_path(path: &Path) -> PathBuf {
    sibling_with_role(path, "invalid")
}

fn sibling_with_role(path: &Path, role: &str) -> PathBuf {
    let stem = path
        .file_stem()
        .and_then(|value| value.to_str())
        .unwrap_or("index");
    let extension = path.extension().and_then(|value| value.to_str());
    let name = match extension {
        Some(extension) => format!("{stem}.{role}.{extension}"),
        None => format!("{stem}.{role}"),
    };
    path.with_file_name(name)
}

fn remove_sqlite_family(path: &Path) -> Result<(), CoreError> {
    if path.is_file() {
        fs::remove_file(path)?;
    }
    remove_auxiliary_files(path)
}

fn remove_auxiliary_files(path: &Path) -> Result<(), CoreError> {
    let Some(name) = path.file_name().and_then(|value| value.to_str()) else {
        return Ok(());
    };
    for suffix in ["-wal", "-shm"] {
        let auxiliary = path.with_file_name(format!("{name}{suffix}"));
        if auxiliary.is_file() {
            fs::remove_file(auxiliary)?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use rusqlite::Connection;

    use super::*;

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
        let first = IndexStore::open(one.path().to_path_buf()).expect("first Index");
        let second = IndexStore::open(two.path().to_path_buf()).expect("second Index");
        assert_eq!(
            schema_fingerprint(first.connection().expect("first connection")),
            schema_fingerprint(second.connection().expect("second connection"))
        );
        let issued_id_tables: u32 = first
            .connection()
            .expect("first connection")
            .query_row(
                "SELECT COUNT(*) FROM sqlite_schema WHERE name='issued_node_ids'",
                [],
                |row| row.get(0),
            )
            .expect("query Index schema");
        assert_eq!(issued_id_tables, 0, "issued identity is never Index state");
    }

    #[test]
    fn unsupported_index_schema_fails_safely() {
        let file = tempfile::NamedTempFile::new().expect("DB");
        let connection = Connection::open(file.path()).expect("open raw DB");
        connection
            .pragma_update(None, "user_version", schema::INDEX_SCHEMA_VERSION + 1)
            .expect("set future schema");
        drop(connection);
        assert!(matches!(
            IndexStore::open(file.path().to_path_buf()),
            Err(CoreError::UnsupportedSchema { .. })
        ));
    }

    #[test]
    fn phase_one_checkpoint_schema_migrates_to_projection_schema() {
        let file = tempfile::NamedTempFile::new().expect("DB");
        let connection = Connection::open(file.path()).expect("open raw DB");
        connection
            .execute_batch(schema::INDEX_SCHEMA_V1)
            .expect("create v1 schema");
        connection
            .execute(
                "INSERT INTO index_meta (singleton, schema_version, minimum_reader_version, applied_record_revision, created_at) \
                 VALUES (1, 1, 1, 0, '2026-09-04T00:00:00Z')",
                [],
            )
            .expect("insert v1 metadata");
        connection
            .pragma_update(None, "user_version", 1)
            .expect("mark v1");
        drop(connection);

        let migrated = IndexStore::open(file.path().to_path_buf()).expect("migrate Index");
        assert_eq!(
            migrated.status().expect("status").schema_version,
            schema::INDEX_SCHEMA_VERSION
        );
        assert_eq!(
            migrated.projection_counts().expect("projection tables"),
            (0, 0, 0)
        );
    }

    #[test]
    fn phase_eight_index_migrates_to_discovery_schema_without_claiming_content_truth() {
        let file = tempfile::NamedTempFile::new().expect("DB");
        let connection = Connection::open(file.path()).expect("open raw DB");
        connection
            .execute_batch(schema::INDEX_SCHEMA_V1)
            .expect("create v1 schema");
        connection
            .execute_batch(schema::INDEX_SCHEMA_V2)
            .expect("create v2 schema");
        connection
            .execute(
                "INSERT INTO index_meta (singleton, schema_version, minimum_reader_version, applied_record_revision, created_at) \
                 VALUES (1, 2, 1, 0, '2026-09-06T00:00:00Z')",
                [],
            )
            .expect("insert v2 metadata");
        connection
            .pragma_update(None, "user_version", 2)
            .expect("mark v2");
        drop(connection);

        let migrated = IndexStore::open(file.path().to_path_buf()).expect("migrate Index");
        assert_eq!(
            migrated.status().expect("status").schema_version,
            schema::INDEX_SCHEMA_VERSION
        );
        let fts_table: u32 = migrated
            .connection()
            .expect("connection")
            .query_row(
                "SELECT COUNT(*) FROM sqlite_schema WHERE name='indexed_search_fts'",
                [],
                |row| row.get(0),
            )
            .expect("FTS schema");
        assert_eq!(fts_table, 1);
        assert!(migrated
            .search("anything", 10)
            .expect("empty search")
            .is_empty());
    }

    #[test]
    fn rebuildable_index_uses_explicit_connection_settings() {
        let file = tempfile::NamedTempFile::new().expect("DB");
        let store = IndexStore::open(file.path().to_path_buf()).expect("initialize Index");
        let connection = store.connection().expect("connection");
        let foreign_keys: u32 = connection
            .pragma_query_value(None, "foreign_keys", |row| row.get(0))
            .expect("foreign_keys");
        let synchronous: u32 = connection
            .pragma_query_value(None, "synchronous", |row| row.get(0))
            .expect("synchronous");
        let busy_timeout: u32 = connection
            .pragma_query_value(None, "busy_timeout", |row| row.get(0))
            .expect("busy_timeout");
        assert_eq!(foreign_keys, 1);
        assert_eq!(synchronous, 1, "NORMAL is the explicit Index policy");
        assert_eq!(busy_timeout, 5_000);
    }
}
