use rusqlite::{Connection, TransactionBehavior};

use super::{schema, STORE_NAME};
use crate::CoreError;

pub(super) fn migrate(connection: &mut Connection) -> Result<(), CoreError> {
    let version: u32 = connection.pragma_query_value(None, "user_version", |row| row.get(0))?;

    // A join-based v4→v5 conversion must never silently drop an orphaned
    // historical fact whose rowid no longer resolves to an identity.
    if (1..schema::RECORD_SCHEMA_VERSION).contains(&version) {
        let violations: i64 =
            connection.query_row("SELECT COUNT(*) FROM pragma_foreign_key_check", [], |row| {
                row.get(0)
            })?;
        if violations != 0 {
            return Err(CoreError::CorruptStore {
                store: STORE_NAME,
                reason:
                    "canonical inconsistency: pre-migration references lack resolvable identities"
                        .into(),
            });
        }
    }

    match version {
        schema::RECORD_SCHEMA_VERSION => Ok(()),
        0 => migrate_empty_to_current(connection),
        1..=3 => adopt_legacy_core_schema_version(connection, version),
        4 => migrate_v4_to_v5(connection),
        found => Err(CoreError::UnsupportedSchema {
            store: STORE_NAME,
            found,
            supported: schema::RECORD_SCHEMA_VERSION,
        }),
    }
}

fn migrate_empty_to_current(connection: &mut Connection) -> Result<(), CoreError> {
    let existing_tables: u32 = connection.query_row(
        "SELECT COUNT(*) FROM sqlite_schema WHERE type = 'table' AND name NOT LIKE 'sqlite_%'",
        [],
        |row| row.get(0),
    )?;
    if existing_tables != 0 {
        return Err(CoreError::CorruptStore {
            store: STORE_NAME,
            reason: "unversioned database already contains tables".into(),
        });
    }

    let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
    transaction.execute_batch(schema::RECORD_SCHEMA_V1)?;
    transaction.execute_batch(schema::RECORD_SCHEMA_V4)?;
    transaction.execute_batch(schema::RECORD_SCHEMA_V5)?;
    transaction.execute(
        "INSERT INTO store_meta (singleton, schema_version, current_revision, minimum_reader_version, created_at) \
         VALUES (1, ?1, 0, ?2, strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))",
        [
            schema::RECORD_SCHEMA_VERSION,
            schema::MINIMUM_RECORD_READER_VERSION,
        ],
    )?;
    transaction.pragma_update(None, "user_version", schema::RECORD_SCHEMA_VERSION)?;
    transaction.commit()?;
    Ok(())
}

/// Versions 2 and 3 historically added first-party capability tables. Core no
/// longer creates or reads those tables, but it continues to accept those
/// version numbers so existing Record Stores remain readable. Extra legacy
/// tables are ignored and preserved for above-Core migration code.
fn adopt_legacy_core_schema_version(
    connection: &mut Connection,
    source_version: u32,
) -> Result<(), CoreError> {
    let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
    transaction.execute_batch(schema::RECORD_SCHEMA_V4)?;
    transaction.execute_batch(schema::RECORD_SCHEMA_V5)?;
    let updated = transaction.execute(
        "UPDATE store_meta SET schema_version=?1, minimum_reader_version=?2 \
         WHERE singleton=1 AND schema_version=?3 \
         AND minimum_reader_version BETWEEN 1 AND ?3 AND current_revision >= 0",
        [
            schema::RECORD_SCHEMA_VERSION,
            schema::MINIMUM_RECORD_READER_VERSION,
            source_version,
        ],
    )?;
    if updated != 1 {
        return Err(CoreError::CorruptStore {
            store: STORE_NAME,
            reason: "legacy store metadata is inconsistent with its schema version".into(),
        });
    }
    transaction.pragma_update(None, "user_version", schema::RECORD_SCHEMA_VERSION)?;
    transaction.commit()?;
    Ok(())
}

fn migrate_v4_to_v5(connection: &mut Connection) -> Result<(), CoreError> {
    let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
    transaction.execute_batch(schema::RECORD_SCHEMA_V5)?;
    let updated = transaction.execute(
        "UPDATE store_meta SET schema_version=?1 WHERE singleton=1 AND schema_version=4",
        [schema::RECORD_SCHEMA_VERSION],
    )?;
    if updated != 1 {
        return Err(CoreError::CorruptStore {
            store: STORE_NAME,
            reason: "v4 metadata is inconsistent with its schema version".into(),
        });
    }
    transaction.pragma_update(None, "user_version", schema::RECORD_SCHEMA_VERSION)?;
    transaction.commit()?;
    Ok(())
}
