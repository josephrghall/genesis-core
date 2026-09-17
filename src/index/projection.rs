use rusqlite::{params, TransactionBehavior};

use super::{FailurePoint, IndexStore};
use crate::record_store::{
    ProjectionNode, ProjectionPatch, ProjectionPropertyDefinition, ProjectionRelationship,
    ProjectionRelationshipDefinition, ProjectionSnapshot,
};
use crate::{CoreError, DefinitionState, PropertyValue, PropertyValueKind, ReconcileOutcome};
use crate::{DerivedRelationshipInput, NodeId, SearchDocumentInput};

impl IndexStore {
    pub(crate) fn replace_content_projection(
        &mut self,
        documents: &[SearchDocumentInput],
        relationships: &[DerivedRelationshipInput],
    ) -> Result<(), CoreError> {
        let transaction = self
            .connection_mut()?
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        transaction.execute_batch(
            "DELETE FROM indexed_search_fts;
             DELETE FROM indexed_search_documents;
             DELETE FROM indexed_derived_relationships;",
        )?;
        for document in documents {
            insert_search_document(&transaction, document)?;
        }
        for relationship in relationships {
            insert_derived_relationship(&transaction, relationship)?;
        }
        transaction.commit()?;
        Ok(())
    }

    pub(crate) fn replace_node_content_projection(
        &mut self,
        node_id: NodeId,
        documents: &[SearchDocumentInput],
        relationships: &[DerivedRelationshipInput],
    ) -> Result<(), CoreError> {
        if documents.iter().any(|document| document.node_id != node_id)
            || relationships
                .iter()
                .any(|relationship| relationship.source != node_id)
        {
            return Err(CoreError::Conflict(
                "incremental content projection contains another Node".into(),
            ));
        }
        let transaction = self
            .connection_mut()?
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let keys = {
            let mut statement = transaction
                .prepare("SELECT document_key FROM indexed_search_documents WHERE node_uuid=?1")?;
            let rows = statement.query_map([node_id.as_bytes().as_slice()], |row| row.get(0))?;
            rows.collect::<Result<Vec<String>, _>>()?
        };
        for key in keys {
            transaction.execute(
                "DELETE FROM indexed_search_fts WHERE document_key=?1",
                [&key],
            )?;
        }
        transaction.execute(
            "DELETE FROM indexed_search_documents WHERE node_uuid=?1",
            [node_id.as_bytes().as_slice()],
        )?;
        transaction.execute(
            "DELETE FROM indexed_derived_relationships WHERE source_uuid=?1",
            [node_id.as_bytes().as_slice()],
        )?;
        for document in documents {
            insert_search_document(&transaction, document)?;
        }
        for relationship in relationships {
            insert_derived_relationship(&transaction, relationship)?;
        }
        transaction.commit()?;
        Ok(())
    }

    pub(super) fn replace_projection(
        &mut self,
        snapshot: &ProjectionSnapshot,
    ) -> Result<(), CoreError> {
        self.fail_if(FailurePoint::DuringRebuild)?;
        let transaction = self
            .connection_mut()?
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        transaction.execute_batch(
            "DELETE FROM indexed_relationships;
             DELETE FROM indexed_properties;
             DELETE FROM indexed_nodes;
             DELETE FROM indexed_property_definitions;
             DELETE FROM indexed_relationship_definitions;",
        )?;
        for node in &snapshot.nodes {
            insert_node(&transaction, node)?;
        }
        for relationship in &snapshot.relationships {
            insert_relationship(&transaction, relationship)?;
        }
        for definition in &snapshot.property_definitions {
            insert_property_definition(&transaction, definition)?;
        }
        for definition in &snapshot.relationship_definitions {
            insert_relationship_definition(&transaction, definition)?;
        }
        transaction.execute(
            "UPDATE index_meta SET applied_record_revision=?1 WHERE singleton=1",
            [snapshot.revision as i64],
        )?;
        transaction.commit()?;
        Ok(())
    }

    pub(super) fn apply_patch(
        &mut self,
        patch: &ProjectionPatch,
    ) -> Result<ReconcileOutcome, CoreError> {
        self.fail_if(FailurePoint::BeforeApply)?;
        let fail_before_checkpoint = self.take_failure(FailurePoint::BeforeCheckpoint);
        let transaction = self
            .connection_mut()?
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        for (id, node) in &patch.nodes {
            if let Some(node) = node {
                insert_node(&transaction, node)?;
            } else {
                transaction.execute(
                    "DELETE FROM indexed_nodes WHERE node_uuid=?1",
                    [id.as_bytes().as_slice()],
                )?;
            }
        }
        for ((source, key, target), relationship) in &patch.relationships {
            transaction.execute(
                "DELETE FROM indexed_relationships WHERE source_uuid=?1 AND relationship_key=?2 AND target_uuid=?3",
                params![source.as_bytes().as_slice(), key, target.as_bytes().as_slice()],
            )?;
            if let Some(relationship) = relationship {
                insert_relationship(&transaction, relationship)?;
            }
        }
        for (key, definition) in &patch.property_definitions {
            transaction.execute(
                "DELETE FROM indexed_property_definitions WHERE key=?1",
                [key],
            )?;
            if let Some(definition) = definition {
                insert_property_definition(&transaction, definition)?;
            }
        }
        for (key, definition) in &patch.relationship_definitions {
            transaction.execute(
                "DELETE FROM indexed_relationship_definitions WHERE key=?1",
                [key],
            )?;
            if let Some(definition) = definition {
                insert_relationship_definition(&transaction, definition)?;
            }
        }
        if fail_before_checkpoint {
            return Err(CoreError::IndexUnavailable(
                "injected failure before checkpoint advancement".into(),
            ));
        }
        transaction.execute(
            "UPDATE index_meta SET applied_record_revision=?1 WHERE singleton=1",
            [patch.target_revision as i64],
        )?;
        transaction.commit()?;
        Ok(ReconcileOutcome::Incremental {
            from_revision: patch.from_revision,
            to_revision: patch.target_revision,
            change_count: patch.change_count,
        })
    }
}

fn insert_search_document(
    transaction: &rusqlite::Transaction<'_>,
    document: &SearchDocumentInput,
) -> Result<(), CoreError> {
    if document.document_key.trim().is_empty() || document.source_kind.trim().is_empty() {
        return Err(CoreError::Conflict(
            "search documents require stable keys and source kinds".into(),
        ));
    }
    transaction.execute(
        "INSERT INTO indexed_search_documents
         (document_key, node_uuid, source_kind, title, body, metadata, source_locator, source_fingerprint)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
        params![
            document.document_key,
            document.node_id.as_bytes().as_slice(),
            document.source_kind,
            document.title,
            document.body,
            document.metadata,
            document.source_locator,
            document.source_fingerprint,
        ],
    )?;
    transaction.execute(
        "INSERT INTO indexed_search_fts
         (document_key, node_uuid, title, body, metadata) VALUES (?1, ?2, ?3, ?4, ?5)",
        params![
            document.document_key,
            document.node_id.to_string(),
            document.title,
            document.body,
            document.metadata,
        ],
    )?;
    Ok(())
}

fn insert_derived_relationship(
    transaction: &rusqlite::Transaction<'_>,
    relationship: &DerivedRelationshipInput,
) -> Result<(), CoreError> {
    if relationship.target_text.trim().is_empty() {
        return Err(CoreError::InvalidRelationship(
            "content-derived relationship requires a resolved or unresolved target".into(),
        ));
    }
    transaction.execute(
        "INSERT INTO indexed_derived_relationships
         (source_uuid, relationship_key, target_uuid, target_text, origin, source_kind,
          source_locator, parser, start_offset, end_offset)
         VALUES (?1, ?2, ?3, ?4, 'content_derived', ?5, ?6, ?7, ?8, ?9)",
        params![
            relationship.source.as_bytes().as_slice(),
            relationship.relationship_key,
            relationship.target.map(|target| target.as_bytes().to_vec()),
            relationship.target_text,
            relationship.source_kind,
            relationship.source_locator,
            relationship.parser,
            i64::try_from(relationship.start_offset).map_err(|_| {
                CoreError::InvalidRelationship("content link offset exceeds SQLite range".into())
            })?,
            i64::try_from(relationship.end_offset).map_err(|_| {
                CoreError::InvalidRelationship("content link offset exceeds SQLite range".into())
            })?,
        ],
    )?;
    Ok(())
}

fn insert_node(
    transaction: &rusqlite::Transaction<'_>,
    node: &ProjectionNode,
) -> Result<(), CoreError> {
    transaction.execute(
        "INSERT INTO indexed_nodes (node_uuid, lifecycle_state, modified_revision) VALUES (?1, ?2, ?3) \
         ON CONFLICT(node_uuid) DO UPDATE SET lifecycle_state=excluded.lifecycle_state, modified_revision=excluded.modified_revision",
        params![
            node.id.as_bytes().as_slice(),
            match node.lifecycle_state {
                crate::LifecycleState::Active => "active",
                crate::LifecycleState::Tombstoned => "tombstoned",
            },
            node.modified_revision as i64
        ],
    )?;
    transaction.execute(
        "DELETE FROM indexed_properties WHERE node_uuid=?1",
        [node.id.as_bytes().as_slice()],
    )?;
    for property in &node.properties {
        let (kind, text, integer, real, json) = encode_value(&property.value)?;
        transaction.execute(
            "INSERT INTO indexed_properties \
             (node_uuid, property_key, value_kind, text_value, integer_value, real_value, json_value, modified_revision) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            params![node.id.as_bytes().as_slice(), property.key, kind, text, integer, real, json, property.modified_revision as i64],
        )?;
    }
    Ok(())
}

fn insert_relationship(
    transaction: &rusqlite::Transaction<'_>,
    relationship: &ProjectionRelationship,
) -> Result<(), CoreError> {
    transaction.execute(
        "INSERT INTO indexed_relationships (source_uuid, relationship_key, target_uuid, modified_revision) VALUES (?1, ?2, ?3, ?4)",
        params![relationship.source.as_bytes().as_slice(), relationship.key, relationship.target.as_bytes().as_slice(), relationship.modified_revision as i64],
    )?;
    Ok(())
}

fn insert_property_definition(
    transaction: &rusqlite::Transaction<'_>,
    projected: &ProjectionPropertyDefinition,
) -> Result<(), CoreError> {
    let definition = &projected.definition;
    transaction.execute(
        "INSERT INTO indexed_property_definitions \
         (key, label, description, expected_value_kind, lifecycle_state, replacement_key, origin, definition_version, modified_revision) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
        params![definition.key, definition.label, definition.description, definition.expected_value_kind.map(kind_to_db), state_to_db(definition.state), definition.replacement_key, definition.origin, definition.version as i64, projected.modified_revision as i64],
    )?;
    Ok(())
}

fn insert_relationship_definition(
    transaction: &rusqlite::Transaction<'_>,
    projected: &ProjectionRelationshipDefinition,
) -> Result<(), CoreError> {
    let definition = &projected.definition;
    transaction.execute(
        "INSERT INTO indexed_relationship_definitions \
         (key, forward_label, inverse_label, description, lifecycle_state, replacement_key, origin, definition_version, modified_revision) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
        params![definition.key, definition.forward_label, definition.inverse_label, definition.description, state_to_db(definition.state), definition.replacement_key, definition.origin, definition.version as i64, projected.modified_revision as i64],
    )?;
    Ok(())
}

type EncodedValue = (
    String,
    Option<String>,
    Option<i64>,
    Option<f64>,
    Option<String>,
);

fn encode_value(value: &PropertyValue) -> Result<EncodedValue, CoreError> {
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

fn state_to_db(state: DefinitionState) -> &'static str {
    match state {
        DefinitionState::Active => "active",
        DefinitionState::Deprecated => "deprecated",
    }
}
