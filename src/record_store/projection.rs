use std::collections::BTreeSet;

use rusqlite::{params, OptionalExtension};
use serde::{Deserialize, Serialize};

use super::{operations, RecordStore};
use crate::{
    CoreError, LifecycleState, NodeId, PropertyDefinition, PropertyValue, RelationshipDefinition,
};

pub(crate) const RECORD_CHANGE_RETENTION_REVISIONS: u64 = 1_024;
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct ProjectionProperty {
    pub key: String,
    pub value: PropertyValue,
    pub modified_revision: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct ProjectionNode {
    pub id: NodeId,
    pub lifecycle_state: LifecycleState,
    pub created_revision: u64,
    pub modified_revision: u64,
    pub properties: Vec<ProjectionProperty>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct ProjectionRelationship {
    pub source: NodeId,
    pub key: String,
    pub target: NodeId,
    pub created_revision: u64,
    pub modified_revision: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct ProjectionPropertyDefinition {
    pub definition: PropertyDefinition,
    pub modified_revision: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct ProjectionRelationshipDefinition {
    pub definition: RelationshipDefinition,
    pub modified_revision: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct ProjectionSnapshot {
    pub revision: u64,
    pub nodes: Vec<ProjectionNode>,
    pub relationships: Vec<ProjectionRelationship>,
    pub property_definitions: Vec<ProjectionPropertyDefinition>,
    pub relationship_definitions: Vec<ProjectionRelationshipDefinition>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct ProjectionPatch {
    pub from_revision: u64,
    pub target_revision: u64,
    pub change_count: usize,
    #[serde(default)]
    pub issued_node_ids: Vec<NodeId>,
    pub nodes: Vec<(NodeId, Option<ProjectionNode>)>,
    pub relationships: Vec<((NodeId, String, NodeId), Option<ProjectionRelationship>)>,
    pub property_definitions: Vec<(String, Option<ProjectionPropertyDefinition>)>,
    pub relationship_definitions: Vec<(String, Option<ProjectionRelationshipDefinition>)>,
}

#[derive(Debug, Clone)]
pub(crate) enum JournalDelta {
    Current(u64),
    Available(ProjectionPatch),
    Unavailable { current_revision: u64 },
}

#[derive(Debug)]
struct ChangeDescriptor {
    revision: u64,
    kind: String,
    node_id: Option<NodeId>,
    definition_key: Option<String>,
    descriptor_json: Option<String>,
}

impl RecordStore {
    pub(crate) fn current_revision(&self) -> Result<u64, CoreError> {
        let revision: i64 = self.connection.query_row(
            "SELECT current_revision FROM store_meta WHERE singleton=1",
            [],
            |row| row.get(0),
        )?;
        Ok(revision as u64)
    }

    pub(crate) fn projection_snapshot(&self) -> Result<ProjectionSnapshot, CoreError> {
        let revision = self.current_revision()?;
        let node_ids = self.query_node_ids("SELECT node_uuid FROM nodes ORDER BY node_uuid", [])?;
        let nodes = node_ids
            .into_iter()
            .map(|id| self.projection_node(id))
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .flatten()
            .collect();

        let mut relationship_statement = self.connection.prepare(
            "SELECT r.source_uuid, r.relationship_key, r.target_uuid, r.created_revision, r.modified_revision \
             FROM relationships r \
             ORDER BY r.source_uuid, r.relationship_key, r.target_uuid",
        )?;
        let relationship_rows = relationship_statement.query_map([], |row| {
            Ok((
                row.get::<_, Vec<u8>>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, Vec<u8>>(2)?,
                row.get::<_, i64>(3)?,
                row.get::<_, i64>(4)?,
            ))
        })?;
        let mut relationships = Vec::new();
        for row in relationship_rows {
            let (source, key, target, created_revision, modified_revision) = row?;
            relationships.push(ProjectionRelationship {
                source: operations::decode_node_id(source)?,
                key,
                target: operations::decode_node_id(target)?,
                created_revision: created_revision as u64,
                modified_revision: modified_revision as u64,
            });
        }

        let mut property_definition_statement = self.connection.prepare(
            "SELECT key, label, description, expected_value_kind, lifecycle_state, replacement_key, origin, definition_version, modified_revision \
             FROM property_definitions ORDER BY key",
        )?;
        let property_definition_rows = property_definition_statement.query_map([], |row| {
            Ok((
                operations::read_property_definition(row)?,
                row.get::<_, i64>(8)?,
            ))
        })?;
        let property_definitions = property_definition_rows
            .map(|row| {
                row.map(
                    |(definition, modified_revision)| ProjectionPropertyDefinition {
                        definition,
                        modified_revision: modified_revision as u64,
                    },
                )
            })
            .collect::<Result<Vec<_>, _>>()?;

        let mut relationship_definition_statement = self.connection.prepare(
            "SELECT key, forward_label, inverse_label, description, lifecycle_state, replacement_key, origin, definition_version, modified_revision \
             FROM relationship_definitions ORDER BY key",
        )?;
        let relationship_definition_rows =
            relationship_definition_statement.query_map([], |row| {
                Ok((
                    operations::read_relationship_definition(row)?,
                    row.get::<_, i64>(8)?,
                ))
            })?;
        let relationship_definitions = relationship_definition_rows
            .map(|row| {
                row.map(
                    |(definition, modified_revision)| ProjectionRelationshipDefinition {
                        definition,
                        modified_revision: modified_revision as u64,
                    },
                )
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(ProjectionSnapshot {
            revision,
            nodes,
            relationships,
            property_definitions,
            relationship_definitions,
        })
    }

    pub(crate) fn projection_delta_since(
        &self,
        checkpoint: u64,
    ) -> Result<JournalDelta, CoreError> {
        let current_revision = self.current_revision()?;
        if checkpoint == current_revision {
            return Ok(JournalDelta::Current(current_revision));
        }
        if checkpoint > current_revision {
            return Ok(JournalDelta::Unavailable { current_revision });
        }

        let mut statement = self.connection.prepare(
            "SELECT revision, change_kind, node_uuid, definition_key, descriptor_json \
             FROM record_changes WHERE revision > ?1 ORDER BY revision, ordinal",
        )?;
        let rows = statement.query_map([checkpoint as i64], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, Option<Vec<u8>>>(2)?,
                row.get::<_, Option<String>>(3)?,
                row.get::<_, Option<String>>(4)?,
            ))
        })?;
        let mut changes = Vec::new();
        for row in rows {
            let (revision, kind, node_uuid, definition_key, descriptor_json) = row?;
            changes.push(ChangeDescriptor {
                revision: revision as u64,
                kind,
                node_id: node_uuid.map(operations::decode_node_id).transpose()?,
                definition_key,
                descriptor_json,
            });
        }
        if !journal_is_contiguous(&changes, checkpoint, current_revision) {
            return Ok(JournalDelta::Unavailable { current_revision });
        }

        let mut node_ids = BTreeSet::new();
        let mut issued_node_ids = BTreeSet::new();
        let mut relationship_keys = BTreeSet::new();
        let mut property_definition_keys = BTreeSet::new();
        let mut relationship_definition_keys = BTreeSet::new();
        for change in &changes {
            match change.kind.as_str() {
                "node_created" => {
                    let Some(node_id) = change.node_id else {
                        return Ok(JournalDelta::Unavailable { current_revision });
                    };
                    node_ids.insert(node_id);
                    issued_node_ids.insert(node_id);
                }
                "node_tombstoned" | "node_restored" | "property_set" | "property_removed" => {
                    let Some(node_id) = change.node_id else {
                        return Ok(JournalDelta::Unavailable { current_revision });
                    };
                    node_ids.insert(node_id);
                }
                "node_purged" => {
                    let Some(node_id) = change.node_id else {
                        return Ok(JournalDelta::Unavailable { current_revision });
                    };
                    node_ids.insert(node_id);
                    issued_node_ids.insert(node_id);
                }
                "relationship_added" | "relationship_removed" => {
                    let (Some(source), Some(key), Some(descriptor)) = (
                        change.node_id,
                        change.definition_key.as_deref(),
                        change.descriptor_json.as_deref(),
                    ) else {
                        return Ok(JournalDelta::Unavailable { current_revision });
                    };
                    let Some(target) = relationship_target(descriptor) else {
                        return Ok(JournalDelta::Unavailable { current_revision });
                    };
                    node_ids.insert(source);
                    node_ids.insert(target);
                    relationship_keys.insert((source, key.to_owned(), target));
                }
                "property_definition_registered"
                | "property_definition_updated"
                | "property_definition_deprecated"
                | "property_definition_removed" => {
                    let Some(key) = &change.definition_key else {
                        return Ok(JournalDelta::Unavailable { current_revision });
                    };
                    property_definition_keys.insert(key.clone());
                }
                "relationship_definition_registered"
                | "relationship_definition_updated"
                | "relationship_definition_deprecated"
                | "relationship_definition_removed" => {
                    let Some(key) = &change.definition_key else {
                        return Ok(JournalDelta::Unavailable { current_revision });
                    };
                    relationship_definition_keys.insert(key.clone());
                }
                _ => return Ok(JournalDelta::Unavailable { current_revision }),
            }
        }

        let nodes = node_ids
            .into_iter()
            .map(|id| Ok((id, self.projection_node(id)?)))
            .collect::<Result<Vec<_>, CoreError>>()?;
        let relationships = relationship_keys
            .into_iter()
            .map(|key| Ok((key.clone(), self.projection_relationship(&key)?)))
            .collect::<Result<Vec<_>, CoreError>>()?;
        let property_definitions = property_definition_keys
            .into_iter()
            .map(|key| Ok((key.clone(), self.projection_property_definition(&key)?)))
            .collect::<Result<Vec<_>, CoreError>>()?;
        let relationship_definitions = relationship_definition_keys
            .into_iter()
            .map(|key| Ok((key.clone(), self.projection_relationship_definition(&key)?)))
            .collect::<Result<Vec<_>, CoreError>>()?;
        Ok(JournalDelta::Available(ProjectionPatch {
            from_revision: checkpoint,
            target_revision: current_revision,
            change_count: changes.len(),
            issued_node_ids: issued_node_ids.into_iter().collect(),
            nodes,
            relationships,
            property_definitions,
            relationship_definitions,
        }))
    }

    pub(crate) fn prune_change_journal(&mut self) -> Result<usize, CoreError> {
        let current = self.current_revision()?;
        if current <= RECORD_CHANGE_RETENTION_REVISIONS {
            return Ok(0);
        }
        let first_to_keep = current - RECORD_CHANGE_RETENTION_REVISIONS + 1;
        Ok(self.connection.execute(
            "DELETE FROM record_changes WHERE revision < ?1",
            [first_to_keep as i64],
        )?)
    }

    #[cfg(test)]
    pub(crate) fn prune_changes_through(&mut self, revision: u64) -> Result<usize, CoreError> {
        Ok(self.connection.execute(
            "DELETE FROM record_changes WHERE revision <= ?1",
            [revision as i64],
        )?)
    }

    fn projection_node(&self, id: NodeId) -> Result<Option<ProjectionNode>, CoreError> {
        let row = self
            .connection
            .query_row(
                "SELECT node_rowid, lifecycle_state, created_revision, modified_revision FROM nodes WHERE node_uuid=?1",
                [id.as_bytes().as_slice()],
                |row| {
                    Ok((
                        row.get::<_, i64>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, i64>(2)?,
                        row.get::<_, i64>(3)?,
                    ))
                },
            )
            .optional()?;
        let Some((rowid, lifecycle_state, created_revision, modified_revision)) = row else {
            return Ok(None);
        };
        let mut property_statement = self.connection.prepare(
            "SELECT property_key, value_kind, text_value, integer_value, real_value, json_value, modified_revision \
             FROM node_properties WHERE node_rowid=?1 ORDER BY property_key",
        )?;
        let property_rows = property_statement.query_map([rowid], |row| {
            Ok((
                row.get::<_, String>(0)?,
                (
                    row.get::<_, String>(1)?,
                    row.get::<_, Option<String>>(2)?,
                    row.get::<_, Option<i64>>(3)?,
                    row.get::<_, Option<f64>>(4)?,
                    row.get::<_, Option<String>>(5)?,
                ),
                row.get::<_, i64>(6)?,
            ))
        })?;
        let mut properties = Vec::new();
        for row in property_rows {
            let (key, encoded, modified_revision) = row?;
            properties.push(ProjectionProperty {
                key,
                value: operations::decode_value(encoded)?,
                modified_revision: modified_revision as u64,
            });
        }
        Ok(Some(ProjectionNode {
            id,
            lifecycle_state: operations::lifecycle_from_db(&lifecycle_state)?,
            created_revision: created_revision as u64,
            modified_revision: modified_revision as u64,
            properties,
        }))
    }

    fn projection_relationship(
        &self,
        key: &(NodeId, String, NodeId),
    ) -> Result<Option<ProjectionRelationship>, CoreError> {
        let row = self
            .connection
            .query_row(
                "SELECT r.created_revision, r.modified_revision FROM relationships r \
             WHERE r.source_uuid=?1 AND r.relationship_key=?2 AND r.target_uuid=?3",
                params![
                    key.0.as_bytes().as_slice(),
                    key.1,
                    key.2.as_bytes().as_slice()
                ],
                |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)),
            )
            .optional()?;
        Ok(row.map(
            |(created_revision, modified_revision)| ProjectionRelationship {
                source: key.0,
                key: key.1.clone(),
                target: key.2,
                created_revision: created_revision as u64,
                modified_revision: modified_revision as u64,
            },
        ))
    }

    fn projection_property_definition(
        &self,
        key: &str,
    ) -> Result<Option<ProjectionPropertyDefinition>, CoreError> {
        let row = self.connection.query_row(
            "SELECT key, label, description, expected_value_kind, lifecycle_state, replacement_key, origin, definition_version, modified_revision \
             FROM property_definitions WHERE key=?1", [key], |row| {
                Ok((operations::read_property_definition(row)?, row.get::<_, i64>(8)?))
             }).optional()?;
        Ok(row.map(
            |(definition, modified_revision)| ProjectionPropertyDefinition {
                definition,
                modified_revision: modified_revision as u64,
            },
        ))
    }

    fn projection_relationship_definition(
        &self,
        key: &str,
    ) -> Result<Option<ProjectionRelationshipDefinition>, CoreError> {
        let row = self.connection.query_row(
            "SELECT key, forward_label, inverse_label, description, lifecycle_state, replacement_key, origin, definition_version, modified_revision \
             FROM relationship_definitions WHERE key=?1", [key], |row| {
                Ok((operations::read_relationship_definition(row)?, row.get::<_, i64>(8)?))
             }).optional()?;
        Ok(row.map(
            |(definition, modified_revision)| ProjectionRelationshipDefinition {
                definition,
                modified_revision: modified_revision as u64,
            },
        ))
    }

    fn query_node_ids<P: rusqlite::Params>(
        &self,
        sql: &str,
        params: P,
    ) -> Result<Vec<NodeId>, CoreError> {
        let mut statement = self.connection.prepare(sql)?;
        let rows = statement.query_map(params, |row| row.get::<_, Vec<u8>>(0))?;
        rows.map(|row| operations::decode_node_id(row?)).collect()
    }
}

fn journal_is_contiguous(
    changes: &[ChangeDescriptor],
    checkpoint: u64,
    current_revision: u64,
) -> bool {
    if changes.first().map(|change| change.revision) != Some(checkpoint + 1)
        || changes.last().map(|change| change.revision) != Some(current_revision)
    {
        return false;
    }
    let mut expected = checkpoint + 1;
    for change in changes {
        if change.revision == expected {
            continue;
        }
        if change.revision == expected + 1 {
            expected += 1;
            continue;
        }
        return false;
    }
    expected == current_revision
}

fn relationship_target(descriptor: &str) -> Option<NodeId> {
    serde_json::from_str::<serde_json::Value>(descriptor)
        .ok()?
        .get("target")?
        .as_str()?
        .parse()
        .ok()
}
