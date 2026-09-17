use rusqlite::{Connection, TransactionBehavior};

use super::{schema, STORE_NAME};
use crate::CoreError;

pub(super) fn migrate(connection: &mut Connection) -> Result<(), CoreError> {
    let version: u32 = connection.pragma_query_value(None, "user_version", |row| row.get(0))?;

    match version {
        schema::INDEX_SCHEMA_VERSION => Ok(()),
        0 => migrate_empty_to_current(connection),
        1 => migrate_v1_to_v2(connection),
        2 => migrate_v2_to_v3(connection),
        3 => migrate_v3_to_v4(connection),
        found => Err(CoreError::UnsupportedSchema {
            store: STORE_NAME,
            found,
            supported: schema::INDEX_SCHEMA_VERSION,
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
    transaction.execute_batch(schema::INDEX_SCHEMA_V1)?;
    transaction.execute_batch(schema::INDEX_SCHEMA_V2)?;
    transaction.execute_batch(schema::INDEX_SCHEMA_V3)?;
    transaction.execute_batch(schema::INDEX_SCHEMA_V4)?;
    transaction.execute(
        "INSERT INTO index_meta (singleton, schema_version, minimum_reader_version, applied_record_revision, created_at) \
         VALUES (1, ?1, ?2, 0, strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))",
        [
            schema::INDEX_SCHEMA_VERSION,
            schema::MINIMUM_INDEX_READER_VERSION,
        ],
    )?;
    transaction.pragma_update(None, "user_version", schema::INDEX_SCHEMA_VERSION)?;
    transaction.commit()?;
    Ok(())
}

fn migrate_v1_to_v2(connection: &mut Connection) -> Result<(), CoreError> {
    let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
    transaction.execute_batch(schema::INDEX_SCHEMA_V2)?;
    transaction.execute_batch(schema::INDEX_SCHEMA_V3)?;
    transaction.execute_batch(schema::INDEX_SCHEMA_V4)?;
    transaction.execute(
        "UPDATE index_meta SET schema_version=?1, minimum_reader_version=?2 WHERE singleton=1",
        [
            schema::INDEX_SCHEMA_VERSION,
            schema::MINIMUM_INDEX_READER_VERSION,
        ],
    )?;
    transaction.pragma_update(None, "user_version", schema::INDEX_SCHEMA_VERSION)?;
    transaction.commit()?;
    Ok(())
}

fn migrate_v2_to_v3(connection: &mut Connection) -> Result<(), CoreError> {
    let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
    transaction.execute_batch(schema::INDEX_SCHEMA_V3)?;
    transaction.execute_batch(schema::INDEX_SCHEMA_V4)?;
    transaction.execute(
        "UPDATE index_meta SET schema_version=?1, minimum_reader_version=?2 WHERE singleton=1",
        [
            schema::INDEX_SCHEMA_VERSION,
            schema::MINIMUM_INDEX_READER_VERSION,
        ],
    )?;
    transaction.pragma_update(None, "user_version", schema::INDEX_SCHEMA_VERSION)?;
    transaction.commit()?;
    Ok(())
}

fn migrate_v3_to_v4(connection: &mut Connection) -> Result<(), CoreError> {
    let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
    transaction.execute_batch(schema::INDEX_SCHEMA_V4)?;
    transaction.execute(
        "UPDATE index_meta SET schema_version=?1, minimum_reader_version=?2 WHERE singleton=1",
        [
            schema::INDEX_SCHEMA_VERSION,
            schema::MINIMUM_INDEX_READER_VERSION,
        ],
    )?;
    transaction.pragma_update(None, "user_version", schema::INDEX_SCHEMA_VERSION)?;
    transaction.commit()?;
    Ok(())
}
