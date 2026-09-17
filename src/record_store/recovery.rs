use std::path::Path;

use rusqlite::{params, TransactionBehavior, MAIN_DB};

use super::{operations, ProjectionPatch, RecordStore};
use crate::{CoreError, DefinitionState, LifecycleState, PropertyValueKind};

impl RecordStore {
    /// Produce a transactionally consistent copy while the canonical store is live.
    pub(crate) fn backup_to(&self, destination: &Path) -> Result<(), CoreError> {
        self.connection.backup(MAIN_DB, destination, None)?;
        Ok(())
    }

    /// Apply a replay-safe final-state frame to a recovery candidate.
    pub(crate) fn apply_recovery_patch(
        &mut self,
        patch: &ProjectionPatch,
    ) -> Result<(), CoreError> {
        let current = self.current_revision()?;
        if current != patch.from_revision || patch.target_revision <= patch.from_revision {
            return Err(CoreError::CorruptStore {
                store: "Record recovery segment",
                reason: format!(
                    "segment {}..{} cannot follow candidate revision {current}",
                    patch.from_revision, patch.target_revision
                ),
            });
        }

        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        transaction.pragma_update(None, "defer_foreign_keys", true)?;

        for id in &patch.issued_node_ids {
            transaction.execute(
                "INSERT INTO issued_node_ids (node_uuid) VALUES (?1) \
                 ON CONFLICT(node_uuid) DO NOTHING",
                [id.as_bytes().as_slice()],
            )?;
        }

        for (id, projected) in &patch.nodes {
            match projected {
                Some(node) => {
                    transaction.execute(
                        "INSERT INTO issued_node_ids (node_uuid) VALUES (?1) \
                         ON CONFLICT(node_uuid) DO NOTHING",
                        [node.id.as_bytes().as_slice()],
                    )?;
                    transaction.execute(
                        "INSERT INTO nodes (node_uuid, lifecycle_state, created_revision, modified_revision) \
                         VALUES (?1, ?2, ?3, ?4) \
                         ON CONFLICT(node_uuid) DO UPDATE SET lifecycle_state=excluded.lifecycle_state, \
                         created_revision=excluded.created_revision, modified_revision=excluded.modified_revision",
                        params![
                            node.id.as_bytes().as_slice(),
                            lifecycle_to_db(node.lifecycle_state),
                            node.created_revision as i64,
                            node.modified_revision as i64
                        ],
                    )?;
                    let rowid: i64 = transaction.query_row(
                        "SELECT node_rowid FROM nodes WHERE node_uuid=?1",
                        [node.id.as_bytes().as_slice()],
                        |row| row.get(0),
                    )?;
                    transaction
                        .execute("DELETE FROM node_properties WHERE node_rowid=?1", [rowid])?;
                    for property in &node.properties {
                        let (kind, text, integer, real, json) =
                            operations::encode_value(&property.value)?;
                        transaction.execute(
                            "INSERT INTO node_properties \
                             (node_rowid, property_key, value_kind, text_value, integer_value, real_value, json_value, modified_revision) \
                             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
                            params![
                                rowid,
                                property.key,
                                kind,
                                text,
                                integer,
                                real,
                                json,
                                property.modified_revision as i64
                            ],
                        )?;
                    }
                }
                None => {
                    transaction.execute(
                        "DELETE FROM nodes WHERE node_uuid=?1",
                        [id.as_bytes().as_slice()],
                    )?;
                }
            }
        }

        for ((source, key, target), projected) in &patch.relationships {
            transaction.execute(
                "DELETE FROM relationships WHERE source_uuid=?1 \
                  AND relationship_key=?2 AND target_uuid=?3",
                params![
                    source.as_bytes().as_slice(),
                    key,
                    target.as_bytes().as_slice()
                ],
            )?;
            if let Some(relationship) = projected {
                transaction.execute(
                    "INSERT INTO relationships \
                     (source_uuid, relationship_key, target_uuid, created_revision, modified_revision) \
                     VALUES (?1, ?2, ?3, ?4, ?5)",
                    params![
                        relationship.source.as_bytes().as_slice(),
                        relationship.key,
                        relationship.target.as_bytes().as_slice(),
                        relationship.created_revision as i64,
                        relationship.modified_revision as i64
                    ],
                )?;
            }
        }

        for (key, projected) in &patch.property_definitions {
            transaction.execute("DELETE FROM property_definitions WHERE key=?1", [key])?;
            if let Some(projected) = projected {
                let definition = &projected.definition;
                transaction.execute(
                    "INSERT INTO property_definitions \
                     (key, label, description, expected_value_kind, lifecycle_state, replacement_key, origin, definition_version, modified_revision) \
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
                    params![
                        definition.key,
                        definition.label,
                        definition.description,
                        definition.expected_value_kind.map(kind_to_db),
                        definition_state_to_db(definition.state),
                        definition.replacement_key,
                        definition.origin,
                        definition.version as i64,
                        projected.modified_revision as i64
                    ],
                )?;
            }
        }

        for (key, projected) in &patch.relationship_definitions {
            transaction.execute("DELETE FROM relationship_definitions WHERE key=?1", [key])?;
            if let Some(projected) = projected {
                let definition = &projected.definition;
                transaction.execute(
                    "INSERT INTO relationship_definitions \
                     (key, forward_label, inverse_label, description, lifecycle_state, replacement_key, origin, definition_version, modified_revision) \
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
                    params![
                        definition.key,
                        definition.forward_label,
                        definition.inverse_label,
                        definition.description,
                        definition_state_to_db(definition.state),
                        definition.replacement_key,
                        definition.origin,
                        definition.version as i64,
                        projected.modified_revision as i64
                    ],
                )?;
            }
        }

        transaction.execute(
            "UPDATE store_meta SET current_revision=?1 WHERE singleton=1",
            [patch.target_revision as i64],
        )?;
        transaction.execute("DELETE FROM record_changes", [])?;
        transaction.commit()?;
        Ok(())
    }

    pub(crate) fn clear_change_journal(&mut self) -> Result<(), CoreError> {
        self.connection.execute("DELETE FROM record_changes", [])?;
        Ok(())
    }

    /// Fold candidate WAL state into the main file before filesystem promotion.
    pub(crate) fn prepare_for_promotion(&self) -> Result<(), CoreError> {
        self.connection
            .execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")?;
        let mode: String = self
            .connection
            .query_row("PRAGMA journal_mode=DELETE", [], |row| row.get(0))?;
        if !mode.eq_ignore_ascii_case("delete") {
            return Err(CoreError::CorruptStore {
                store: "Record recovery candidate",
                reason: format!("could not consolidate candidate journal; mode is {mode}"),
            });
        }
        Ok(())
    }
}

fn lifecycle_to_db(state: LifecycleState) -> &'static str {
    match state {
        LifecycleState::Active => "active",
        LifecycleState::Tombstoned => "tombstoned",
    }
}

fn definition_state_to_db(state: DefinitionState) -> &'static str {
    match state {
        DefinitionState::Active => "active",
        DefinitionState::Deprecated => "deprecated",
    }
}

fn kind_to_db(kind: PropertyValueKind) -> &'static str {
    match kind {
        PropertyValueKind::Null => "null",
        PropertyValueKind::Boolean => "boolean",
        PropertyValueKind::Integer => "integer",
        PropertyValueKind::Real => "real",
        PropertyValueKind::Text => "text",
        PropertyValueKind::Date => "date",
        PropertyValueKind::DateTime => "date_time",
        PropertyValueKind::List => "list",
        PropertyValueKind::Structured => "structured",
    }
}
