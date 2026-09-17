use std::collections::BTreeMap;

use rusqlite::{params, OptionalExtension, Transaction, TransactionBehavior};
use serde_json::json;

use super::RecordStore;
use crate::{
    Commit, CoreError, DefinitionState, DefinitionUsage, IdentityResolution, LifecycleState,
    Mutation, NewPropertyDefinition, NewRelationshipDefinition, Node, NodeId, PropertyDefinition,
    PropertyDefinitionUpdate, PropertyValue, PropertyValueKind, ReadPage, Relationship,
    RelationshipDefinition, RelationshipDefinitionUpdate, RelationshipDirection,
};

impl RecordStore {
    pub(crate) fn create_node(&mut self) -> Result<Node, CoreError> {
        let node = Node::blank();
        self.create_node_with_id(node.id)
    }

    pub(crate) fn create_node_with_id(&mut self, node_id: NodeId) -> Result<Node, CoreError> {
        let node = Node {
            id: node_id,
            lifecycle_state: LifecycleState::Active,
        };
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        reserve_node_id(&transaction, node_id)?;
        let revision = next_revision(&transaction)?;
        transaction.execute(
            "INSERT INTO nodes (node_uuid, lifecycle_state, created_revision, modified_revision) \
             VALUES (?1, 'active', ?2, ?2)",
            params![node.id.as_bytes().as_slice(), revision],
        )?;
        journal(
            &transaction,
            revision,
            0,
            "node_created",
            Some(node.id),
            None,
            None,
        )?;
        transaction.commit()?;
        Ok(node)
    }

    pub(crate) fn create_nodes(&mut self, count: usize) -> Result<Vec<Node>, CoreError> {
        if count == 0 {
            return Err(CoreError::Conflict(
                "the Node batch size must be greater than zero".into(),
            ));
        }
        let nodes = (0..count).map(|_| Node::blank()).collect::<Vec<_>>();
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        for node in &nodes {
            reserve_node_id(&transaction, node.id)?;
        }
        let revision = next_revision(&transaction)?;
        for (ordinal, node) in nodes.iter().enumerate() {
            transaction.execute(
                "INSERT INTO nodes (node_uuid, lifecycle_state, created_revision, modified_revision)                  VALUES (?1, 'active', ?2, ?2)",
                params![node.id.as_bytes().as_slice(), revision],
            )?;
            journal(
                &transaction,
                revision,
                ordinal,
                "node_created",
                Some(node.id),
                None,
                None,
            )?;
        }
        transaction.commit()?;
        Ok(nodes)
    }

    pub(crate) fn get_node(&self, id: NodeId) -> Result<Node, CoreError> {
        let state = self
            .connection
            .query_row(
                "SELECT lifecycle_state FROM nodes WHERE node_uuid = ?1",
                [id.as_bytes().as_slice()],
                |row| row.get::<_, String>(0),
            )
            .optional()?
            .ok_or_else(|| not_found("Node", id))?;
        Ok(Node {
            id,
            lifecycle_state: lifecycle_from_db(&state)?,
        })
    }

    pub(crate) fn resolve_identity(&self, id: NodeId) -> Result<IdentityResolution, CoreError> {
        let lifecycle = self
            .connection
            .query_row(
                "SELECT lifecycle_state FROM nodes WHERE node_uuid = ?1",
                [id.as_bytes().as_slice()],
                |row| row.get::<_, String>(0),
            )
            .optional()?;
        match lifecycle.as_deref() {
            Some("active") => Ok(IdentityResolution::Active),
            Some("tombstoned") => Ok(IdentityResolution::Tombstoned),
            Some(_) => Err(CoreError::CorruptStore {
                store: "Record Store",
                reason: format!("Node {id} has an invalid lifecycle state"),
            }),
            None => {
                let issued: bool = self.connection.query_row(
                    "SELECT EXISTS(SELECT 1 FROM issued_node_ids WHERE node_uuid=?1)",
                    [id.as_bytes().as_slice()],
                    |row| row.get(0),
                )?;
                Ok(if issued {
                    IdentityResolution::Retired
                } else {
                    IdentityResolution::NeverIssued
                })
            }
        }
    }

    pub(crate) fn get_property(
        &self,
        node_id: NodeId,
        key: &str,
    ) -> Result<Option<PropertyValue>, CoreError> {
        validate_key(key, "property")?;
        require_node(&self.connection, node_id)?;
        let encoded = self
            .connection
            .query_row(
                "SELECT value_kind, text_value, integer_value, real_value, json_value \
                 FROM node_properties p JOIN nodes n ON n.node_rowid = p.node_rowid \
                 WHERE n.node_uuid = ?1 AND p.property_key = ?2",
                params![node_id.as_bytes().as_slice(), key],
                read_encoded_value,
            )
            .optional()?;
        encoded.map(decode_value).transpose()
    }

    pub(crate) fn list_properties(
        &self,
        node_id: NodeId,
    ) -> Result<BTreeMap<String, PropertyValue>, CoreError> {
        require_node(&self.connection, node_id)?;
        let mut statement = self.connection.prepare(
            "SELECT property_key, value_kind, text_value, integer_value, real_value, json_value \
             FROM node_properties p JOIN nodes n ON n.node_rowid = p.node_rowid \
             WHERE n.node_uuid = ?1 ORDER BY property_key",
        )?;
        let rows = statement.query_map([node_id.as_bytes().as_slice()], |row| {
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
        })?;
        let mut properties = BTreeMap::new();
        for row in rows {
            let (key, encoded) = row?;
            properties.insert(key, decode_value(encoded)?);
        }
        Ok(properties)
    }

    pub(crate) fn list_properties_page(
        &self,
        node_id: NodeId,
        page: ReadPage,
    ) -> Result<Vec<(String, PropertyValue)>, CoreError> {
        require_node(&self.connection, node_id)?;
        let mut statement = self.connection.prepare(
            "SELECT property_key, value_kind, text_value, integer_value, real_value, json_value \
             FROM node_properties p JOIN nodes n ON n.node_rowid = p.node_rowid \
             WHERE n.node_uuid = ?1 ORDER BY property_key LIMIT ?2 OFFSET ?3",
        )?;
        let rows = statement.query_map(
            params![
                node_id.as_bytes().as_slice(),
                page.sql_limit(),
                page.sql_offset()?
            ],
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
        rows.map(|row| {
            let (key, encoded) = row?;
            Ok((key, decode_value(encoded)?))
        })
        .collect()
    }

    pub(crate) fn query_relationships(
        &self,
        node_id: NodeId,
        direction: RelationshipDirection,
        key: Option<&str>,
    ) -> Result<Vec<Relationship>, CoreError> {
        require_issued_id(&self.connection, node_id)?;
        if let Some(key) = key {
            validate_key(key, "relationship")?;
        }
        let (predicate, order) = match direction {
            RelationshipDirection::Outgoing => {
                ("r.source_uuid = ?1", "r.target_uuid, r.relationship_key")
            }
            RelationshipDirection::Incoming => {
                ("r.target_uuid = ?1", "r.source_uuid, r.relationship_key")
            }
            RelationshipDirection::Either => (
                "(r.source_uuid = ?1 OR r.target_uuid = ?1)",
                "r.source_uuid, r.relationship_key, r.target_uuid",
            ),
        };
        let sql = format!(
            "SELECT r.source_uuid, r.relationship_key, r.target_uuid \
             FROM relationships r \
             WHERE {predicate} AND (?2 IS NULL OR r.relationship_key = ?2) ORDER BY {order}"
        );
        let mut statement = self.connection.prepare(&sql)?;
        let rows = statement.query_map(params![node_id.as_bytes().as_slice(), key], |row| {
            Ok((
                row.get::<_, Vec<u8>>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, Vec<u8>>(2)?,
            ))
        })?;
        let mut relationships = Vec::new();
        for row in rows {
            let (source, key, target) = row?;
            relationships.push(Relationship {
                source: decode_node_id(source)?,
                key,
                target: decode_node_id(target)?,
            });
        }
        Ok(relationships)
    }

    pub(crate) fn query_relationships_page(
        &self,
        node_id: NodeId,
        direction: RelationshipDirection,
        key: Option<&str>,
        page: ReadPage,
    ) -> Result<Vec<Relationship>, CoreError> {
        require_issued_id(&self.connection, node_id)?;
        if let Some(key) = key {
            validate_key(key, "relationship")?;
        }
        let (predicate, order) = match direction {
            RelationshipDirection::Outgoing => {
                ("r.source_uuid = ?1", "r.target_uuid, r.relationship_key")
            }
            RelationshipDirection::Incoming => {
                ("r.target_uuid = ?1", "r.source_uuid, r.relationship_key")
            }
            RelationshipDirection::Either => (
                "(r.source_uuid = ?1 OR r.target_uuid = ?1)",
                "r.source_uuid, r.relationship_key, r.target_uuid",
            ),
        };
        let sql = format!(
            "SELECT r.source_uuid, r.relationship_key, r.target_uuid FROM relationships r \
             WHERE {predicate} AND (?2 IS NULL OR r.relationship_key = ?2) \
             ORDER BY {order} LIMIT ?3 OFFSET ?4"
        );
        let mut statement = self.connection.prepare(&sql)?;
        let rows = statement.query_map(
            params![
                node_id.as_bytes().as_slice(),
                key,
                page.sql_limit(),
                page.sql_offset()?
            ],
            |row| {
                Ok((
                    row.get::<_, Vec<u8>>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, Vec<u8>>(2)?,
                ))
            },
        )?;
        rows.map(|row| {
            let (source, key, target) = row?;
            Ok(Relationship {
                source: decode_node_id(source)?,
                key,
                target: decode_node_id(target)?,
            })
        })
        .collect()
    }

    pub(crate) fn apply_batch(&mut self, mutations: &[Mutation]) -> Result<Commit, CoreError> {
        if mutations.is_empty() {
            return Err(CoreError::Conflict(
                "an atomic mutation batch cannot be empty".into(),
            ));
        }
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let revision = next_revision(&transaction)?;
        for (ordinal, mutation) in mutations.iter().enumerate() {
            apply_mutation(&transaction, revision, ordinal, mutation)?;
        }
        transaction.commit()?;
        Ok(Commit {
            revision: revision as u64,
            change_count: mutations.len(),
        })
    }

    pub(crate) fn register_property_definition(
        &mut self,
        definition: &NewPropertyDefinition,
    ) -> Result<PropertyDefinition, CoreError> {
        validate_definition_fields(&definition.key, &definition.label, &definition.origin)?;
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        if definition_exists(&transaction, "property_definitions", &definition.key)? {
            return Err(already_exists("Property definition", &definition.key));
        }
        let revision = next_revision(&transaction)?;
        transaction.execute(
            "INSERT INTO property_definitions \
             (key, label, description, expected_value_kind, lifecycle_state, replacement_key, origin, definition_version, modified_revision) \
             VALUES (?1, ?2, ?3, ?4, 'active', NULL, ?5, 1, ?6)",
            params![definition.key, definition.label, definition.description, definition.expected_value_kind.map(kind_to_db), definition.origin, revision],
        )?;
        journal(
            &transaction,
            revision,
            0,
            "property_definition_registered",
            None,
            Some(&definition.key),
            None,
        )?;
        transaction.commit()?;
        Ok(PropertyDefinition {
            key: definition.key.clone(),
            label: definition.label.clone(),
            description: definition.description.clone(),
            expected_value_kind: definition.expected_value_kind,
            state: DefinitionState::Active,
            replacement_key: None,
            origin: definition.origin.clone(),
            version: 1,
        })
    }

    pub(crate) fn get_property_definition(
        &self,
        key: &str,
    ) -> Result<PropertyDefinition, CoreError> {
        validate_key(key, "definition")?;
        self.connection.query_row(
            "SELECT key, label, description, expected_value_kind, lifecycle_state, replacement_key, origin, definition_version \
             FROM property_definitions WHERE key = ?1", [key], read_property_definition,
        ).optional()?.ok_or_else(|| CoreError::DefinitionNotFound(key.into()))
    }

    pub(crate) fn list_property_definitions(&self) -> Result<Vec<PropertyDefinition>, CoreError> {
        let mut statement = self.connection.prepare(
            "SELECT key, label, description, expected_value_kind, lifecycle_state, replacement_key, origin, definition_version \
             FROM property_definitions ORDER BY key")?;
        let definitions = statement
            .query_map([], read_property_definition)?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(definitions)
    }

    pub(crate) fn list_property_definitions_page(
        &self,
        page: ReadPage,
    ) -> Result<Vec<PropertyDefinition>, CoreError> {
        let mut statement = self.connection.prepare(
            "SELECT key, label, description, expected_value_kind, lifecycle_state, replacement_key, origin, definition_version \
             FROM property_definitions ORDER BY key LIMIT ?1 OFFSET ?2",
        )?;
        let rows = statement.query_map(
            params![page.sql_limit(), page.sql_offset()?],
            read_property_definition,
        )?;
        Ok(rows.collect::<Result<Vec<_>, _>>()?)
    }

    pub(crate) fn update_property_definition(
        &mut self,
        key: &str,
        update: &PropertyDefinitionUpdate,
    ) -> Result<PropertyDefinition, CoreError> {
        validate_definition_fields(key, &update.label, "existing")?;
        let current = self.get_property_definition(key)?;
        if current.expected_value_kind != update.expected_value_kind
            && self.property_definition_usage(key)?.fact_count > 0
        {
            return Err(CoreError::Conflict(
                "expected value kind cannot change while durable facts use the definition".into(),
            ));
        }
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let revision = next_revision(&transaction)?;
        transaction.execute(
            "UPDATE property_definitions SET label = ?2, description = ?3, expected_value_kind = ?4, \
             definition_version = definition_version + 1, modified_revision = ?5 WHERE key = ?1",
            params![key, update.label, update.description, update.expected_value_kind.map(kind_to_db), revision],
        )?;
        journal(
            &transaction,
            revision,
            0,
            "property_definition_updated",
            None,
            Some(key),
            None,
        )?;
        transaction.commit()?;
        Ok(PropertyDefinition {
            key: current.key,
            label: update.label.clone(),
            description: update.description.clone(),
            expected_value_kind: update.expected_value_kind,
            state: current.state,
            replacement_key: current.replacement_key,
            origin: current.origin,
            version: current.version + 1,
        })
    }

    pub(crate) fn deprecate_property_definition(
        &mut self,
        key: &str,
        replacement: Option<&str>,
    ) -> Result<PropertyDefinition, CoreError> {
        let current = self.get_property_definition(key)?;
        validate_replacement(&self.connection, "property_definitions", key, replacement)?;
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let revision = next_revision(&transaction)?;
        transaction.execute(
            "UPDATE property_definitions SET lifecycle_state = 'deprecated', replacement_key = ?2, \
             definition_version = definition_version + 1, modified_revision = ?3 WHERE key = ?1",
            params![key, replacement, revision],
        )?;
        journal(
            &transaction,
            revision,
            0,
            "property_definition_deprecated",
            None,
            Some(key),
            replacement.map(|value| json!({"replacement_key": value})),
        )?;
        transaction.commit()?;
        Ok(PropertyDefinition {
            key: current.key,
            label: current.label,
            description: current.description,
            expected_value_kind: current.expected_value_kind,
            state: DefinitionState::Deprecated,
            replacement_key: replacement.map(str::to_owned),
            origin: current.origin,
            version: current.version + 1,
        })
    }

    pub(crate) fn property_definition_usage(
        &self,
        key: &str,
    ) -> Result<DefinitionUsage, CoreError> {
        self.get_property_definition(key)?;
        let count: i64 = self.connection.query_row(
            "SELECT COUNT(*) FROM node_properties WHERE property_key = ?1",
            [key],
            |row| row.get(0),
        )?;
        Ok(DefinitionUsage {
            key: key.into(),
            fact_count: count as u64,
        })
    }

    pub(crate) fn remove_property_definition(&mut self, key: &str) -> Result<Commit, CoreError> {
        ensure_definition_unused(
            &self.connection,
            "property_definitions",
            "node_properties",
            "property_key",
            key,
        )?;
        remove_definition(
            &mut self.connection,
            "property_definitions",
            "property_definition_removed",
            key,
        )
    }

    pub(crate) fn register_relationship_definition(
        &mut self,
        definition: &NewRelationshipDefinition,
    ) -> Result<RelationshipDefinition, CoreError> {
        validate_definition_fields(
            &definition.key,
            &definition.forward_label,
            &definition.origin,
        )?;
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        if definition_exists(&transaction, "relationship_definitions", &definition.key)? {
            return Err(already_exists("Relationship definition", &definition.key));
        }
        let revision = next_revision(&transaction)?;
        transaction.execute(
            "INSERT INTO relationship_definitions \
             (key, forward_label, inverse_label, description, lifecycle_state, replacement_key, origin, definition_version, modified_revision) \
             VALUES (?1, ?2, ?3, ?4, 'active', NULL, ?5, 1, ?6)",
            params![definition.key, definition.forward_label, definition.inverse_label.as_deref().unwrap_or(""), definition.description, definition.origin, revision],
        )?;
        journal(
            &transaction,
            revision,
            0,
            "relationship_definition_registered",
            None,
            Some(&definition.key),
            None,
        )?;
        transaction.commit()?;
        Ok(RelationshipDefinition {
            key: definition.key.clone(),
            forward_label: definition.forward_label.clone(),
            inverse_label: definition.inverse_label.clone(),
            description: definition.description.clone(),
            state: DefinitionState::Active,
            replacement_key: None,
            origin: definition.origin.clone(),
            version: 1,
        })
    }

    pub(crate) fn get_relationship_definition(
        &self,
        key: &str,
    ) -> Result<RelationshipDefinition, CoreError> {
        validate_key(key, "definition")?;
        self.connection.query_row(
            "SELECT key, forward_label, inverse_label, description, lifecycle_state, replacement_key, origin, definition_version \
             FROM relationship_definitions WHERE key = ?1", [key], read_relationship_definition,
        ).optional()?.ok_or_else(|| CoreError::DefinitionNotFound(key.into()))
    }

    pub(crate) fn list_relationship_definitions(
        &self,
    ) -> Result<Vec<RelationshipDefinition>, CoreError> {
        let mut statement = self.connection.prepare(
            "SELECT key, forward_label, inverse_label, description, lifecycle_state, replacement_key, origin, definition_version \
             FROM relationship_definitions ORDER BY key")?;
        let definitions = statement
            .query_map([], read_relationship_definition)?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(definitions)
    }

    pub(crate) fn list_relationship_definitions_page(
        &self,
        page: ReadPage,
    ) -> Result<Vec<RelationshipDefinition>, CoreError> {
        let mut statement = self.connection.prepare(
            "SELECT key, forward_label, inverse_label, description, lifecycle_state, replacement_key, origin, definition_version \
             FROM relationship_definitions ORDER BY key LIMIT ?1 OFFSET ?2",
        )?;
        let rows = statement.query_map(
            params![page.sql_limit(), page.sql_offset()?],
            read_relationship_definition,
        )?;
        Ok(rows.collect::<Result<Vec<_>, _>>()?)
    }

    pub(crate) fn update_relationship_definition(
        &mut self,
        key: &str,
        update: &RelationshipDefinitionUpdate,
    ) -> Result<RelationshipDefinition, CoreError> {
        validate_definition_fields(key, &update.forward_label, "existing")?;
        let current = self.get_relationship_definition(key)?;
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let revision = next_revision(&transaction)?;
        transaction.execute(
            "UPDATE relationship_definitions SET forward_label = ?2, inverse_label = ?3, description = ?4, \
             definition_version = definition_version + 1, modified_revision = ?5 WHERE key = ?1",
            params![key, update.forward_label, update.inverse_label.as_deref().unwrap_or(""), update.description, revision],
        )?;
        journal(
            &transaction,
            revision,
            0,
            "relationship_definition_updated",
            None,
            Some(key),
            None,
        )?;
        transaction.commit()?;
        Ok(RelationshipDefinition {
            key: current.key,
            forward_label: update.forward_label.clone(),
            inverse_label: update.inverse_label.clone(),
            description: update.description.clone(),
            state: current.state,
            replacement_key: current.replacement_key,
            origin: current.origin,
            version: current.version + 1,
        })
    }

    pub(crate) fn deprecate_relationship_definition(
        &mut self,
        key: &str,
        replacement: Option<&str>,
    ) -> Result<RelationshipDefinition, CoreError> {
        let current = self.get_relationship_definition(key)?;
        validate_replacement(
            &self.connection,
            "relationship_definitions",
            key,
            replacement,
        )?;
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let revision = next_revision(&transaction)?;
        transaction.execute(
            "UPDATE relationship_definitions SET lifecycle_state = 'deprecated', replacement_key = ?2, \
             definition_version = definition_version + 1, modified_revision = ?3 WHERE key = ?1",
            params![key, replacement, revision],
        )?;
        journal(
            &transaction,
            revision,
            0,
            "relationship_definition_deprecated",
            None,
            Some(key),
            replacement.map(|value| json!({"replacement_key": value})),
        )?;
        transaction.commit()?;
        Ok(RelationshipDefinition {
            key: current.key,
            forward_label: current.forward_label,
            inverse_label: current.inverse_label,
            description: current.description,
            state: DefinitionState::Deprecated,
            replacement_key: replacement.map(str::to_owned),
            origin: current.origin,
            version: current.version + 1,
        })
    }

    pub(crate) fn relationship_definition_usage(
        &self,
        key: &str,
    ) -> Result<DefinitionUsage, CoreError> {
        self.get_relationship_definition(key)?;
        let count: i64 = self.connection.query_row(
            "SELECT COUNT(*) FROM relationships WHERE relationship_key = ?1",
            [key],
            |row| row.get(0),
        )?;
        Ok(DefinitionUsage {
            key: key.into(),
            fact_count: count as u64,
        })
    }

    pub(crate) fn remove_relationship_definition(
        &mut self,
        key: &str,
    ) -> Result<Commit, CoreError> {
        ensure_definition_unused(
            &self.connection,
            "relationship_definitions",
            "relationships",
            "relationship_key",
            key,
        )?;
        remove_definition(
            &mut self.connection,
            "relationship_definitions",
            "relationship_definition_removed",
            key,
        )
    }
}

pub(super) type EncodedValue = (
    String,
    Option<String>,
    Option<i64>,
    Option<f64>,
    Option<String>,
);

fn apply_mutation(
    transaction: &Transaction<'_>,
    revision: i64,
    ordinal: usize,
    mutation: &Mutation,
) -> Result<(), CoreError> {
    match mutation {
        Mutation::CreateNode { node_id } => {
            reserve_node_id(transaction, *node_id)?;
            transaction.execute(
                "INSERT INTO nodes (node_uuid, lifecycle_state, created_revision, modified_revision) \
                 VALUES (?1, 'active', ?2, ?2)",
                params![node_id.as_bytes().as_slice(), revision],
            )?;
            journal(
                transaction,
                revision,
                ordinal,
                "node_created",
                Some(*node_id),
                None,
                None,
            )?;
        }
        Mutation::SetProperty {
            node_id,
            key,
            value,
        } => {
            validate_key(key, "property")?;
            value.validate()?;
            let rowid = require_active_node(transaction, *node_id)?;
            validate_property_definition(transaction, key, value.kind())?;
            let (kind, text, integer, real, json) = encode_value(value)?;
            transaction.execute(
                "INSERT INTO node_properties (node_rowid, property_key, value_kind, text_value, integer_value, real_value, json_value, modified_revision) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8) \
                 ON CONFLICT(node_rowid, property_key) DO UPDATE SET value_kind=excluded.value_kind, text_value=excluded.text_value, \
                 integer_value=excluded.integer_value, real_value=excluded.real_value, json_value=excluded.json_value, modified_revision=excluded.modified_revision",
                params![rowid, key, kind, text, integer, real, json, revision],
            )?;
            transaction.execute(
                "UPDATE nodes SET modified_revision = ?2 WHERE node_rowid = ?1",
                params![rowid, revision],
            )?;
            journal(
                transaction,
                revision,
                ordinal,
                "property_set",
                Some(*node_id),
                Some(key),
                None,
            )?;
        }
        Mutation::RemoveProperty { node_id, key } => {
            validate_key(key, "property")?;
            let rowid = require_active_node(transaction, *node_id)?;
            let changed = transaction.execute(
                "DELETE FROM node_properties WHERE node_rowid = ?1 AND property_key = ?2",
                params![rowid, key],
            )?;
            if changed == 0 {
                return Err(not_found("Property", key));
            }
            transaction.execute(
                "UPDATE nodes SET modified_revision = ?2 WHERE node_rowid = ?1",
                params![rowid, revision],
            )?;
            journal(
                transaction,
                revision,
                ordinal,
                "property_removed",
                Some(*node_id),
                Some(key),
                None,
            )?;
        }
        Mutation::AddRelationship {
            source,
            key,
            target,
        } => {
            validate_key(key, "relationship")?;
            let source_rowid = require_active_node(transaction, *source)?;
            let target_rowid = require_active_node(transaction, *target)?;
            validate_relationship_definition(transaction, key)?;
            let exists: bool = transaction.query_row(
                "SELECT EXISTS(SELECT 1 FROM relationships WHERE source_uuid=?1 AND relationship_key=?2 AND target_uuid=?3)",
                params![source.as_bytes().as_slice(), key, target.as_bytes().as_slice()], |row| row.get(0))?;
            if exists {
                return Err(already_exists(
                    "Relationship",
                    format!("{source} --{key}--> {target}"),
                ));
            }
            transaction.execute(
                "INSERT INTO relationships (source_uuid, relationship_key, target_uuid, created_revision, modified_revision) VALUES (?1, ?2, ?3, ?4, ?4)",
                params![source.as_bytes().as_slice(), key, target.as_bytes().as_slice(), revision])?;
            transaction.execute(
                "UPDATE nodes SET modified_revision=?3 WHERE node_rowid IN (?1, ?2)",
                params![source_rowid, target_rowid, revision],
            )?;
            journal(
                transaction,
                revision,
                ordinal,
                "relationship_added",
                Some(*source),
                Some(key),
                Some(json!({"target": target})),
            )?;
        }
        Mutation::RemoveRelationship {
            source,
            key,
            target,
        } => {
            validate_key(key, "relationship")?;
            require_issued_id(transaction, *source)?;
            require_issued_id(transaction, *target)?;
            let changed = transaction.execute(
                "DELETE FROM relationships WHERE source_uuid=?1 AND relationship_key=?2 AND target_uuid=?3",
                params![source.as_bytes().as_slice(), key, target.as_bytes().as_slice()])?;
            if changed == 0 {
                return Err(not_found(
                    "Relationship",
                    format!("{source} --{key}--> {target}"),
                ));
            }
            transaction.execute(
                "UPDATE nodes SET modified_revision=?3 WHERE node_uuid IN (?1, ?2)",
                params![
                    source.as_bytes().as_slice(),
                    target.as_bytes().as_slice(),
                    revision
                ],
            )?;
            journal(
                transaction,
                revision,
                ordinal,
                "relationship_removed",
                Some(*source),
                Some(key),
                Some(json!({"target": target})),
            )?;
        }
        Mutation::TombstoneNode { node_id } => {
            let rowid = require_node_state(transaction, *node_id, LifecycleState::Active)?;
            transaction.execute("UPDATE nodes SET lifecycle_state='tombstoned', modified_revision=?2 WHERE node_rowid=?1", params![rowid, revision])?;
            journal(
                transaction,
                revision,
                ordinal,
                "node_tombstoned",
                Some(*node_id),
                None,
                None,
            )?;
        }
        Mutation::RestoreNode { node_id } => {
            let rowid = require_node_state(transaction, *node_id, LifecycleState::Tombstoned)?;
            transaction.execute("UPDATE nodes SET lifecycle_state='active', modified_revision=?2 WHERE node_rowid=?1", params![rowid, revision])?;
            journal(
                transaction,
                revision,
                ordinal,
                "node_restored",
                Some(*node_id),
                None,
                None,
            )?;
        }
        Mutation::PurgeNode { node_id } => {
            let rowid = require_node_state(transaction, *node_id, LifecycleState::Tombstoned)?;
            journal(
                transaction,
                revision,
                ordinal,
                "node_purged",
                Some(*node_id),
                None,
                None,
            )?;
            transaction.execute("DELETE FROM nodes WHERE node_rowid=?1", [rowid])?;
        }
    }
    Ok(())
}

pub(super) fn next_revision(transaction: &Transaction<'_>) -> Result<i64, CoreError> {
    Ok(transaction.query_row(
        "UPDATE store_meta SET current_revision = current_revision + 1 WHERE singleton = 1 RETURNING current_revision",
        [], |row| row.get(0))?)
}

pub(super) fn journal(
    transaction: &Transaction<'_>,
    revision: i64,
    ordinal: usize,
    kind: &str,
    node_id: Option<NodeId>,
    definition_key: Option<&str>,
    descriptor: Option<serde_json::Value>,
) -> Result<(), CoreError> {
    transaction.execute(
        "INSERT INTO record_changes (revision, ordinal, change_kind, node_uuid, definition_key, descriptor_json) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
        params![revision, ordinal as i64, kind, node_id.map(|id| id.into_bytes().to_vec()), definition_key, descriptor.map(|value| value.to_string())])?;
    Ok(())
}

fn require_node(connection: &rusqlite::Connection, id: NodeId) -> Result<i64, CoreError> {
    connection
        .query_row(
            "SELECT node_rowid FROM nodes WHERE node_uuid=?1",
            [id.as_bytes().as_slice()],
            |row| row.get(0),
        )
        .optional()?
        .ok_or_else(|| not_found("Node", id))
}

fn require_issued_id(connection: &rusqlite::Connection, id: NodeId) -> Result<(), CoreError> {
    let issued: bool = connection.query_row(
        "SELECT EXISTS(SELECT 1 FROM issued_node_ids WHERE node_uuid=?1)",
        [id.as_bytes().as_slice()],
        |row| row.get(0),
    )?;
    if issued {
        Ok(())
    } else {
        Err(not_found("issued Node ID", id))
    }
}

fn require_active_node(connection: &rusqlite::Connection, id: NodeId) -> Result<i64, CoreError> {
    require_node_state(connection, id, LifecycleState::Active)
}

fn require_node_state(
    connection: &rusqlite::Connection,
    id: NodeId,
    expected: LifecycleState,
) -> Result<i64, CoreError> {
    let result = connection
        .query_row(
            "SELECT node_rowid, lifecycle_state FROM nodes WHERE node_uuid=?1",
            [id.as_bytes().as_slice()],
            |row| Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?)),
        )
        .optional()?;
    let (rowid, state) = result.ok_or_else(|| not_found("Node", id))?;
    let actual = lifecycle_from_db(&state)?;
    if actual != expected {
        return Err(CoreError::Conflict(format!(
            "Node {id} is {actual:?}; operation requires {expected:?}"
        )));
    }
    Ok(rowid)
}

fn validate_key(key: &str, kind: &str) -> Result<(), CoreError> {
    if key.is_empty()
        || key.len() > 255
        || key.trim() != key
        || key.chars().any(|character| character.is_control())
    {
        return Err(match kind {
            "relationship" => CoreError::InvalidRelationship(format!("invalid key {key:?}")),
            _ => CoreError::InvalidProperty(format!("invalid key {key:?}")),
        });
    }
    Ok(())
}

fn validate_definition_fields(key: &str, label: &str, origin: &str) -> Result<(), CoreError> {
    validate_key(key, "definition")?;
    if label.trim().is_empty() || origin.trim().is_empty() {
        return Err(CoreError::Conflict(
            "definition label and origin must be non-empty".into(),
        ));
    }
    Ok(())
}

fn validate_property_definition(
    connection: &rusqlite::Connection,
    key: &str,
    actual: PropertyValueKind,
) -> Result<(), CoreError> {
    let definition = connection
        .query_row(
            "SELECT expected_value_kind, lifecycle_state FROM property_definitions WHERE key=?1",
            [key],
            |row| Ok((row.get::<_, Option<String>>(0)?, row.get::<_, String>(1)?)),
        )
        .optional()?;
    if let Some((expected, state)) = definition {
        if state == "deprecated" {
            return Err(CoreError::Conflict(format!(
                "property definition {key} is deprecated"
            )));
        }
        if let Some(expected) = expected {
            let expected = kind_from_db(&expected)?;
            if expected != actual {
                return Err(CoreError::InvalidPropertyValue(format!(
                    "property {key} expects {expected:?}, received {actual:?}"
                )));
            }
        }
    }
    Ok(())
}

fn validate_relationship_definition(
    connection: &rusqlite::Connection,
    key: &str,
) -> Result<(), CoreError> {
    let state = connection
        .query_row(
            "SELECT lifecycle_state FROM relationship_definitions WHERE key=?1",
            [key],
            |row| row.get::<_, String>(0),
        )
        .optional()?;
    if state.as_deref() == Some("deprecated") {
        return Err(CoreError::Conflict(format!(
            "relationship definition {key} is deprecated"
        )));
    }
    Ok(())
}

pub(super) fn encode_value(value: &PropertyValue) -> Result<EncodedValue, CoreError> {
    value.validate()?;
    Ok(match value {
        PropertyValue::Null => ("null".into(), None, None, None, None),
        PropertyValue::Boolean(value) => {
            ("boolean".into(), None, Some(i64::from(*value)), None, None)
        }
        PropertyValue::Integer(value) => ("integer".into(), None, Some(*value), None, None),
        PropertyValue::Real(value) => ("real".into(), None, None, Some(*value), None),
        PropertyValue::Text(value) => ("text".into(), Some(value.clone()), None, None, None),
        PropertyValue::Date(value) => ("date".into(), Some(value.clone()), None, None, None),
        PropertyValue::DateTime(value) => {
            ("date_time".into(), Some(value.clone()), None, None, None)
        }
        PropertyValue::List(_) => ("list".into(), None, None, None, value.canonical_json()),
        PropertyValue::Structured(_) => (
            "structured".into(),
            None,
            None,
            None,
            value.canonical_json(),
        ),
    })
}

fn read_encoded_value(row: &rusqlite::Row<'_>) -> rusqlite::Result<EncodedValue> {
    Ok((
        row.get(0)?,
        row.get(1)?,
        row.get(2)?,
        row.get(3)?,
        row.get(4)?,
    ))
}

pub(super) fn decode_value(
    (kind, text, integer, real, json): EncodedValue,
) -> Result<PropertyValue, CoreError> {
    let corrupt = || CoreError::CorruptStore {
        store: "Record Store",
        reason: format!("invalid encoded {kind} Property value"),
    };
    match kind.as_str() {
        "null" => Ok(PropertyValue::Null),
        "boolean" => Ok(PropertyValue::Boolean(integer.ok_or_else(&corrupt)? != 0)),
        "integer" => Ok(PropertyValue::Integer(integer.ok_or_else(&corrupt)?)),
        "real" => Ok(PropertyValue::Real(real.ok_or_else(&corrupt)?)),
        "text" => Ok(PropertyValue::Text(text.ok_or_else(&corrupt)?)),
        "date" => Ok(PropertyValue::Date(text.ok_or_else(&corrupt)?)),
        "date_time" => Ok(PropertyValue::DateTime(text.ok_or_else(&corrupt)?)),
        "list" => Ok(PropertyValue::List(
            serde_json::from_str(&json.ok_or_else(&corrupt)?).map_err(|error| {
                CoreError::CorruptStore {
                    store: "Record Store",
                    reason: error.to_string(),
                }
            })?,
        )),
        "structured" => Ok(PropertyValue::Structured(
            serde_json::from_str(&json.ok_or_else(&corrupt)?).map_err(|error| {
                CoreError::CorruptStore {
                    store: "Record Store",
                    reason: error.to_string(),
                }
            })?,
        )),
        _ => Err(corrupt()),
    }
}

pub(super) fn lifecycle_from_db(value: &str) -> Result<LifecycleState, CoreError> {
    match value {
        "active" => Ok(LifecycleState::Active),
        "tombstoned" => Ok(LifecycleState::Tombstoned),
        _ => Err(CoreError::CorruptStore {
            store: "Record Store",
            reason: format!("unknown lifecycle state {value}"),
        }),
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

pub(super) fn kind_from_db(value: &str) -> Result<PropertyValueKind, CoreError> {
    match value {
        "null" => Ok(PropertyValueKind::Null),
        "boolean" => Ok(PropertyValueKind::Boolean),
        "integer" => Ok(PropertyValueKind::Integer),
        "real" => Ok(PropertyValueKind::Real),
        "text" => Ok(PropertyValueKind::Text),
        "date" => Ok(PropertyValueKind::Date),
        "date_time" => Ok(PropertyValueKind::DateTime),
        "list" => Ok(PropertyValueKind::List),
        "structured" => Ok(PropertyValueKind::Structured),
        _ => Err(CoreError::CorruptStore {
            store: "Record Store",
            reason: format!("unknown Property value kind {value}"),
        }),
    }
}

pub(super) fn definition_state_from_db(value: &str) -> rusqlite::Result<DefinitionState> {
    match value {
        "active" => Ok(DefinitionState::Active),
        "deprecated" => Ok(DefinitionState::Deprecated),
        _ => Err(rusqlite::Error::InvalidQuery),
    }
}

pub(super) fn read_property_definition(
    row: &rusqlite::Row<'_>,
) -> rusqlite::Result<PropertyDefinition> {
    let kind = row
        .get::<_, Option<String>>(3)?
        .map(|value| kind_from_db(&value).map_err(|_| rusqlite::Error::InvalidQuery))
        .transpose()?;
    Ok(PropertyDefinition {
        key: row.get(0)?,
        label: row.get(1)?,
        description: row.get(2)?,
        expected_value_kind: kind,
        state: definition_state_from_db(&row.get::<_, String>(4)?)?,
        replacement_key: row.get(5)?,
        origin: row.get(6)?,
        version: row.get::<_, i64>(7)? as u64,
    })
}

pub(super) fn read_relationship_definition(
    row: &rusqlite::Row<'_>,
) -> rusqlite::Result<RelationshipDefinition> {
    let inverse: String = row.get(2)?;
    Ok(RelationshipDefinition {
        key: row.get(0)?,
        forward_label: row.get(1)?,
        inverse_label: (!inverse.is_empty()).then_some(inverse),
        description: row.get(3)?,
        state: definition_state_from_db(&row.get::<_, String>(4)?)?,
        replacement_key: row.get(5)?,
        origin: row.get(6)?,
        version: row.get::<_, i64>(7)? as u64,
    })
}

fn definition_exists(
    connection: &rusqlite::Connection,
    table: &str,
    key: &str,
) -> Result<bool, CoreError> {
    let sql = format!("SELECT EXISTS(SELECT 1 FROM {table} WHERE key=?1)");
    Ok(connection.query_row(&sql, [key], |row| row.get(0))?)
}

fn validate_replacement(
    connection: &rusqlite::Connection,
    table: &str,
    key: &str,
    replacement: Option<&str>,
) -> Result<(), CoreError> {
    if let Some(replacement) = replacement {
        validate_key(replacement, "definition")?;
        if replacement == key {
            return Err(CoreError::Conflict(
                "a definition cannot replace itself".into(),
            ));
        }
        if !definition_exists(connection, table, replacement)? {
            return Err(CoreError::DefinitionNotFound(replacement.into()));
        }
        let sql = format!("SELECT lifecycle_state FROM {table} WHERE key=?1");
        let state: String = connection.query_row(&sql, [replacement], |row| row.get(0))?;
        if state != "active" {
            return Err(CoreError::Conflict(format!(
                "replacement definition {replacement} must be active"
            )));
        }
    }
    Ok(())
}

fn ensure_definition_unused(
    connection: &rusqlite::Connection,
    definition_table: &str,
    fact_table: &str,
    fact_key: &str,
    key: &str,
) -> Result<(), CoreError> {
    if !definition_exists(connection, definition_table, key)? {
        return Err(CoreError::DefinitionNotFound(key.into()));
    }
    let sql = format!("SELECT (SELECT COUNT(*) FROM {fact_table} WHERE {fact_key}=?1) + (SELECT COUNT(*) FROM {definition_table} WHERE replacement_key=?1)");
    let uses: i64 = connection.query_row(&sql, [key], |row| row.get(0))?;
    if uses > 0 {
        return Err(CoreError::Conflict(format!(
            "definition {key} is still referenced {uses} time(s)"
        )));
    }
    Ok(())
}

fn remove_definition(
    connection: &mut rusqlite::Connection,
    table: &str,
    change_kind: &str,
    key: &str,
) -> Result<Commit, CoreError> {
    let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let revision = next_revision(&transaction)?;
    let sql = format!("DELETE FROM {table} WHERE key=?1");
    transaction.execute(&sql, [key])?;
    journal(
        &transaction,
        revision,
        0,
        change_kind,
        None,
        Some(key),
        None,
    )?;
    transaction.commit()?;
    Ok(Commit {
        revision: revision as u64,
        change_count: 1,
    })
}

pub(super) fn decode_node_id(value: Vec<u8>) -> Result<NodeId, CoreError> {
    let bytes: [u8; 16] = value.try_into().map_err(|_| CoreError::CorruptStore {
        store: "Record Store",
        reason: "Node UUID is not 16 bytes".into(),
    })?;
    NodeId::from_bytes(bytes).map_err(|error| CoreError::CorruptStore {
        store: "Record Store",
        reason: error.to_string(),
    })
}

fn not_found(entity: &'static str, identifier: impl ToString) -> CoreError {
    CoreError::NotFound {
        entity,
        identifier: identifier.to_string(),
    }
}

fn already_exists(entity: &'static str, identifier: impl ToString) -> CoreError {
    CoreError::AlreadyExists {
        entity,
        identifier: identifier.to_string(),
    }
}

fn reserve_node_id(transaction: &Transaction<'_>, node_id: NodeId) -> Result<(), CoreError> {
    let inserted = transaction.execute(
        "INSERT OR IGNORE INTO issued_node_ids (node_uuid) VALUES (?1)",
        [node_id.as_bytes().as_slice()],
    )?;
    if inserted != 1 {
        return Err(already_exists("Issued Node ID", node_id));
    }
    Ok(())
}
