use rusqlite::{params, Connection, TransactionBehavior};

use super::{operations, RecordStore};
use crate::{
    api::{bounded_limit, decode_cursor, encode_cursor},
    CoreError, CursorPage, Node, NodeId, PropertyDefinition, PropertyValue, ReadCursor,
    Relationship, RelationshipDefinition,
};

/// A canonical Record Store read transaction pinned to one SQLite snapshot.
///
/// The value is only available inside `CoreState::with_stable_read_snapshot`.
/// Every page read through it observes exactly the same committed database
/// state, even when another Core connection commits concurrently.
pub struct StableReadSnapshot<'a> {
    connection: &'a Connection,
    revision: u64,
}

impl RecordStore {
    pub(crate) fn with_stable_read_snapshot<T, F>(&mut self, read: F) -> Result<T, CoreError>
    where
        F: FnOnce(&StableReadSnapshot<'_>) -> Result<T, CoreError>,
    {
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Deferred)?;
        let revision: i64 = transaction.query_row(
            "SELECT current_revision FROM store_meta WHERE singleton=1",
            [],
            |row| row.get(0),
        )?;
        let revision = u64::try_from(revision).map_err(|_| CoreError::CorruptStore {
            store: "Record Store",
            reason: "current revision is negative".into(),
        })?;
        let result = {
            let snapshot = StableReadSnapshot {
                connection: &transaction,
                revision,
            };
            read(&snapshot)
        };
        let rollback = transaction.rollback();
        match (result, rollback) {
            (Err(error), _) => Err(error),
            (Ok(value), Ok(())) => Ok(value),
            (Ok(_), Err(error)) => Err(CoreError::Database(error)),
        }
    }
}

impl StableReadSnapshot<'_> {
    pub fn revision(&self) -> u64 {
        self.revision
    }

    /// Traverses every issued identity, including retired IDs, in UUID order.
    pub fn issued_ids_page(
        &self,
        limit: usize,
        cursor: Option<&ReadCursor>,
    ) -> Result<CursorPage<NodeId>, CoreError> {
        const COLLECTION: &str = "record_snapshot.issued_ids.uuid_asc";
        let after = self.after_node_id(cursor, COLLECTION)?;
        let limit = bounded_limit(limit);
        let fetch_limit = (limit + 1) as i64;
        let mut statement = self.connection.prepare(
            "SELECT node_uuid FROM issued_node_ids \
             WHERE (?1 IS NULL OR node_uuid > ?1) ORDER BY node_uuid ASC LIMIT ?2",
        )?;
        let rows = statement.query_map(
            params![
                after.as_ref().map(|id| id.as_bytes().as_slice()),
                fetch_limit
            ],
            |row| row.get::<_, Vec<u8>>(0),
        )?;
        let mut items = rows
            .map(|row| operations::decode_node_id(row?))
            .collect::<Result<Vec<_>, _>>()?;
        self.finish_page(COLLECTION, limit, &mut items, |id| vec![id.to_string()])
    }

    /// Traverses canonical Nodes in permanent UUID order.
    pub fn nodes_page(
        &self,
        limit: usize,
        cursor: Option<&ReadCursor>,
    ) -> Result<CursorPage<Node>, CoreError> {
        const COLLECTION: &str = "record_snapshot.nodes.uuid_asc";
        let after = self.after_node_id(cursor, COLLECTION)?;
        let limit = bounded_limit(limit);
        let fetch_limit = (limit + 1) as i64;
        let mut statement = self.connection.prepare(
            "SELECT node_uuid, lifecycle_state FROM nodes \
             WHERE (?1 IS NULL OR node_uuid > ?1) ORDER BY node_uuid ASC LIMIT ?2",
        )?;
        let rows = statement.query_map(
            params![
                after.as_ref().map(|id| id.as_bytes().as_slice()),
                fetch_limit
            ],
            |row| Ok((row.get::<_, Vec<u8>>(0)?, row.get::<_, String>(1)?)),
        )?;
        let mut items = Vec::new();
        for row in rows {
            let (id, lifecycle) = row?;
            items.push(Node {
                id: operations::decode_node_id(id)?,
                lifecycle_state: operations::lifecycle_from_db(&lifecycle)?,
            });
        }
        self.finish_page(COLLECTION, limit, &mut items, |node| {
            vec![node.id.to_string()]
        })
    }

    /// Traverses one Node's canonical Properties in key order.
    pub fn properties_page(
        &self,
        node_id: NodeId,
        limit: usize,
        cursor: Option<&ReadCursor>,
    ) -> Result<CursorPage<(String, PropertyValue)>, CoreError> {
        let exists: bool = self.connection.query_row(
            "SELECT EXISTS(SELECT 1 FROM nodes WHERE node_uuid=?1)",
            [node_id.as_bytes().as_slice()],
            |row| row.get(0),
        )?;
        if !exists {
            return Err(CoreError::NotFound {
                entity: "Node",
                identifier: node_id.to_string(),
            });
        }
        let collection = format!("record_snapshot.properties.{node_id}.key_asc");
        let after = self.after_string(cursor, &collection)?;
        let limit = bounded_limit(limit);
        let fetch_limit = (limit + 1) as i64;
        let mut statement = self.connection.prepare(
            "SELECT property_key, value_kind, text_value, integer_value, real_value, json_value \
             FROM node_properties p JOIN nodes n ON n.node_rowid=p.node_rowid \
             WHERE n.node_uuid=?1 AND (?2 IS NULL OR property_key > ?2) \
             ORDER BY property_key ASC LIMIT ?3",
        )?;
        let rows = statement.query_map(
            params![node_id.as_bytes().as_slice(), after, fetch_limit],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    (
                        row.get::<_, String>(1)?,
                        row.get::<_, Option<String>>(2)?,
                        row.get::<_, Option<i64>>(3)?,
                        row.get::<_, Option<f64>>(4)?,
                        row.get::<_, Option<String>>(5)?,
                    ),
                ))
            },
        )?;
        let mut items = rows
            .map(|row| {
                let (key, value) = row?;
                Ok((key, operations::decode_value(value)?))
            })
            .collect::<Result<Vec<_>, CoreError>>()?;
        self.finish_page(&collection, limit, &mut items, |(key, _)| vec![key.clone()])
    }

    /// Traverses all canonical Relationships in source/key/target order.
    pub fn relationships_page(
        &self,
        limit: usize,
        cursor: Option<&ReadCursor>,
    ) -> Result<CursorPage<Relationship>, CoreError> {
        const COLLECTION: &str = "record_snapshot.relationships.source_key_target_asc";
        let after = self.after_relationship(cursor, COLLECTION)?;
        let limit = bounded_limit(limit);
        let fetch_limit = (limit + 1) as i64;
        let mut items = Vec::new();
        if let Some((source, key, target)) = after {
            let mut statement = self.connection.prepare(
                "SELECT source_uuid, relationship_key, target_uuid FROM relationships \
                 WHERE source_uuid > ?1 \
                    OR (source_uuid = ?1 AND relationship_key > ?2) \
                    OR (source_uuid = ?1 AND relationship_key = ?2 AND target_uuid > ?3) \
                 ORDER BY source_uuid, relationship_key, target_uuid LIMIT ?4",
            )?;
            let rows = statement.query_map(
                params![
                    source.as_bytes().as_slice(),
                    key,
                    target.as_bytes().as_slice(),
                    fetch_limit
                ],
                |row| {
                    Ok((
                        row.get::<_, Vec<u8>>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, Vec<u8>>(2)?,
                    ))
                },
            )?;
            for row in rows {
                let (source, key, target) = row?;
                items.push(Relationship {
                    source: operations::decode_node_id(source)?,
                    key,
                    target: operations::decode_node_id(target)?,
                });
            }
        } else {
            let mut statement = self.connection.prepare(
                "SELECT source_uuid, relationship_key, target_uuid FROM relationships \
                 ORDER BY source_uuid, relationship_key, target_uuid LIMIT ?1",
            )?;
            let rows = statement.query_map([fetch_limit], |row| {
                Ok((
                    row.get::<_, Vec<u8>>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, Vec<u8>>(2)?,
                ))
            })?;
            for row in rows {
                let (source, key, target) = row?;
                items.push(Relationship {
                    source: operations::decode_node_id(source)?,
                    key,
                    target: operations::decode_node_id(target)?,
                });
            }
        }
        self.finish_page(COLLECTION, limit, &mut items, |relationship| {
            vec![
                relationship.source.to_string(),
                relationship.key.clone(),
                relationship.target.to_string(),
            ]
        })
    }

    pub fn property_definitions_page(
        &self,
        limit: usize,
        cursor: Option<&ReadCursor>,
    ) -> Result<CursorPage<PropertyDefinition>, CoreError> {
        const COLLECTION: &str = "record_snapshot.property_definitions.key_asc";
        let after = self.after_string(cursor, COLLECTION)?;
        let limit = bounded_limit(limit);
        let fetch_limit = (limit + 1) as i64;
        let mut statement = self.connection.prepare(
            "SELECT key, label, description, expected_value_kind, lifecycle_state, replacement_key, origin, definition_version \
             FROM property_definitions WHERE (?1 IS NULL OR key > ?1) \
             ORDER BY key ASC LIMIT ?2",
        )?;
        let rows = statement.query_map(
            params![after, fetch_limit],
            operations::read_property_definition,
        )?;
        let mut items = rows.collect::<Result<Vec<_>, _>>()?;
        self.finish_page(COLLECTION, limit, &mut items, |definition| {
            vec![definition.key.clone()]
        })
    }

    pub fn relationship_definitions_page(
        &self,
        limit: usize,
        cursor: Option<&ReadCursor>,
    ) -> Result<CursorPage<RelationshipDefinition>, CoreError> {
        const COLLECTION: &str = "record_snapshot.relationship_definitions.key_asc";
        let after = self.after_string(cursor, COLLECTION)?;
        let limit = bounded_limit(limit);
        let fetch_limit = (limit + 1) as i64;
        let mut statement = self.connection.prepare(
            "SELECT key, forward_label, inverse_label, description, lifecycle_state, replacement_key, origin, definition_version \
             FROM relationship_definitions WHERE (?1 IS NULL OR key > ?1) \
             ORDER BY key ASC LIMIT ?2",
        )?;
        let rows = statement.query_map(
            params![after, fetch_limit],
            operations::read_relationship_definition,
        )?;
        let mut items = rows.collect::<Result<Vec<_>, _>>()?;
        self.finish_page(COLLECTION, limit, &mut items, |definition| {
            vec![definition.key.clone()]
        })
    }

    fn finish_page<T, F>(
        &self,
        collection: &str,
        limit: usize,
        items: &mut Vec<T>,
        key: F,
    ) -> Result<CursorPage<T>, CoreError>
    where
        F: Fn(&T) -> Vec<String>,
    {
        let has_more = items.len() > limit;
        items.truncate(limit);
        let next_cursor = if has_more {
            Some(encode_cursor(
                collection,
                self.revision,
                key(items
                    .last()
                    .expect("a page with a continuation contains at least one row")),
            )?)
        } else {
            None
        };
        Ok(CursorPage {
            items: std::mem::take(items),
            next_cursor,
        })
    }

    fn payload(
        &self,
        cursor: Option<&ReadCursor>,
        collection: &str,
    ) -> Result<Option<Vec<String>>, CoreError> {
        cursor
            .map(|cursor| {
                let payload = decode_cursor(cursor, collection)?;
                if payload.revision != self.revision {
                    return Err(CoreError::Conflict(
                        "read cursor belongs to a different stable snapshot".into(),
                    ));
                }
                Ok(payload.after)
            })
            .transpose()
    }

    fn after_string(
        &self,
        cursor: Option<&ReadCursor>,
        collection: &str,
    ) -> Result<Option<String>, CoreError> {
        self.payload(cursor, collection)?
            .map(|after| {
                if after.len() != 1 {
                    return Err(CoreError::Conflict(
                        "invalid or unsupported read cursor".into(),
                    ));
                }
                Ok(after[0].clone())
            })
            .transpose()
    }

    fn after_node_id(
        &self,
        cursor: Option<&ReadCursor>,
        collection: &str,
    ) -> Result<Option<NodeId>, CoreError> {
        self.after_string(cursor, collection)?
            .map(|value| {
                value
                    .parse::<NodeId>()
                    .map_err(|_| CoreError::Conflict("invalid or unsupported read cursor".into()))
            })
            .transpose()
    }

    fn after_relationship(
        &self,
        cursor: Option<&ReadCursor>,
        collection: &str,
    ) -> Result<Option<(NodeId, String, NodeId)>, CoreError> {
        self.payload(cursor, collection)?
            .map(|after| {
                if after.len() != 3 {
                    return Err(CoreError::Conflict(
                        "invalid or unsupported read cursor".into(),
                    ));
                }
                let source = after[0].parse::<NodeId>().map_err(|_| {
                    CoreError::Conflict("invalid or unsupported read cursor".into())
                })?;
                let target = after[2].parse::<NodeId>().map_err(|_| {
                    CoreError::Conflict("invalid or unsupported read cursor".into())
                })?;
                Ok((source, after[1].clone(), target))
            })
            .transpose()
    }
}
