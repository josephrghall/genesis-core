use serde::{Deserialize, Serialize};

use super::{NodeId, PropertyValue, PropertyValueKind};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DefinitionState {
    Active,
    Deprecated,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PropertyDefinition {
    pub key: String,
    pub label: String,
    pub description: Option<String>,
    pub expected_value_kind: Option<PropertyValueKind>,
    pub state: DefinitionState,
    pub replacement_key: Option<String>,
    pub origin: String,
    pub version: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NewPropertyDefinition {
    pub key: String,
    pub label: String,
    pub description: Option<String>,
    pub expected_value_kind: Option<PropertyValueKind>,
    pub origin: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PropertyDefinitionUpdate {
    pub label: String,
    pub description: Option<String>,
    pub expected_value_kind: Option<PropertyValueKind>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RelationshipDefinition {
    pub key: String,
    pub forward_label: String,
    pub inverse_label: Option<String>,
    pub description: Option<String>,
    pub state: DefinitionState,
    pub replacement_key: Option<String>,
    pub origin: String,
    pub version: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NewRelationshipDefinition {
    pub key: String,
    pub forward_label: String,
    pub inverse_label: Option<String>,
    pub description: Option<String>,
    pub origin: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RelationshipDefinitionUpdate {
    pub forward_label: String,
    pub inverse_label: Option<String>,
    pub description: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DefinitionUsage {
    pub key: String,
    pub fact_count: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Relationship {
    pub source: NodeId,
    pub key: String,
    pub target: NodeId,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RelationshipDirection {
    Outgoing,
    Incoming,
    Either,
}

/// Canonical resolution of a syntactically valid Node ID.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IdentityResolution {
    Active,
    Tombstoned,
    Retired,
    NeverIssued,
}

/// Opaque continuation token for keyset traversal.
///
/// The token is transportable and serializable, but its contents and encoding
/// are not part of Core's compatibility contract. Callers must return it
/// unchanged to the operation that produced it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ReadCursor(String);

impl ReadCursor {
    /// Rehydrates a token received from a previous Core response.
    /// Validation is performed when the cursor is used.
    pub fn from_token(token: impl Into<String>) -> Self {
        Self(token.into())
    }

    pub fn token(&self) -> &str {
        &self.0
    }
}

/// One bounded keyset page and its optional continuation token.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CursorPage<T> {
    pub items: Vec<T>,
    pub next_cursor: Option<ReadCursor>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct CursorPayload {
    pub version: u8,
    pub collection: String,
    pub revision: u64,
    pub after: Vec<String>,
}

pub(crate) fn bounded_limit(limit: usize) -> usize {
    limit.clamp(1, ReadPage::MAX_LIMIT)
}

pub(crate) fn encode_cursor(
    collection: &str,
    revision: u64,
    after: Vec<String>,
) -> Result<ReadCursor, crate::CoreError> {
    let payload = CursorPayload {
        version: 1,
        collection: collection.into(),
        revision,
        after,
    };
    let json = serde_json::to_vec(&payload)
        .map_err(|error| crate::CoreError::Conflict(format!("cursor encoding failed: {error}")))?;
    let mut token = String::with_capacity(json.len() * 2);
    for byte in json {
        use std::fmt::Write as _;
        write!(&mut token, "{byte:02x}").expect("writing to String cannot fail");
    }
    Ok(ReadCursor(token))
}

pub(crate) fn decode_cursor(
    cursor: &ReadCursor,
    expected_collection: &str,
) -> Result<CursorPayload, crate::CoreError> {
    let token = cursor.token();
    if token.is_empty() || token.len() > 8_192 || !token.len().is_multiple_of(2) {
        return Err(crate::CoreError::Conflict(
            "invalid or unsupported read cursor".into(),
        ));
    }
    let bytes = token
        .as_bytes()
        .chunks(2)
        .map(|pair| {
            let text = std::str::from_utf8(pair).map_err(|_| {
                crate::CoreError::Conflict("invalid or unsupported read cursor".into())
            })?;
            u8::from_str_radix(text, 16).map_err(|_| {
                crate::CoreError::Conflict("invalid or unsupported read cursor".into())
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    let payload: CursorPayload = serde_json::from_slice(&bytes)
        .map_err(|_| crate::CoreError::Conflict("invalid or unsupported read cursor".into()))?;
    if payload.version != 1 || payload.collection != expected_collection {
        return Err(crate::CoreError::Conflict(
            "read cursor does not match this traversal contract".into(),
        ));
    }
    Ok(payload)
}

/// Bounded OFFSET compatibility window. Limits above 1,000 are clamped.
///
/// New sequential traversal should use a cursor API. OFFSET remains available
/// for existing callers and small random-access windows, but it is not a
/// stable long-running traversal contract when concurrent writes occur.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReadPage {
    pub offset: u64,
    pub limit: usize,
}

impl ReadPage {
    pub const MAX_LIMIT: usize = 1_000;

    pub const fn new(offset: u64, limit: usize) -> Self {
        Self { offset, limit }
    }

    pub(crate) fn sql_limit(self) -> i64 {
        self.limit.clamp(1, Self::MAX_LIMIT) as i64
    }

    pub(crate) fn sql_offset(self) -> Result<i64, crate::CoreError> {
        i64::try_from(self.offset).map_err(|_| {
            crate::CoreError::Conflict("read-page offset exceeds the supported range".into())
        })
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "operation", rename_all = "snake_case")]
pub enum Mutation {
    /// Atomically issues and creates a blank active Node. Later mutations in
    /// the same batch may initialize its Properties and Relationships.
    CreateNode {
        node_id: NodeId,
    },
    SetProperty {
        node_id: NodeId,
        key: String,
        value: PropertyValue,
    },
    RemoveProperty {
        node_id: NodeId,
        key: String,
    },
    AddRelationship {
        source: NodeId,
        key: String,
        target: NodeId,
    },
    RemoveRelationship {
        source: NodeId,
        key: String,
        target: NodeId,
    },
    TombstoneNode {
        node_id: NodeId,
    },
    RestoreNode {
        node_id: NodeId,
    },
    PurgeNode {
        node_id: NodeId,
    },
}

/// Generic comparison supported by the Index-backed Property query path.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum PropertyFilter {
    Equals(PropertyValue),
    AtLeast(PropertyValue),
    AtMost(PropertyValue),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PropertySort {
    Ascending,
    Descending,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PropertyQuery {
    pub key: String,
    pub filter: Option<PropertyFilter>,
    pub sort: PropertySort,
    pub limit: usize,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IndexedPropertyMatch {
    pub node_id: NodeId,
    pub value: PropertyValue,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Commit {
    pub revision: u64,
    pub change_count: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IndexSyncState {
    Current,
    Stale,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IndexSyncStatus {
    pub record_revision: u64,
    pub applied_revision: u64,
    pub state: IndexSyncState,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IndexRebuildReason {
    Missing,
    CorruptOrIncompatible,
    JournalUnavailable,
    Requested,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "outcome", rename_all = "snake_case")]
pub enum ReconcileOutcome {
    Current {
        revision: u64,
    },
    Incremental {
        from_revision: u64,
        to_revision: u64,
        change_count: usize,
    },
    Rebuilt {
        revision: u64,
        reason: IndexRebuildReason,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SearchDocumentInput {
    pub document_key: String,
    pub node_id: NodeId,
    pub source_kind: String,
    pub title: String,
    pub body: String,
    pub metadata: String,
    pub source_locator: Option<String>,
    pub source_fingerprint: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DerivedRelationshipInput {
    pub source: NodeId,
    pub relationship_key: String,
    pub target: Option<NodeId>,
    pub target_text: String,
    pub source_kind: String,
    pub source_locator: Option<String>,
    pub parser: String,
    pub start_offset: usize,
    pub end_offset: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RelationshipProvenance {
    CanonicalRecord,
    ContentDerived,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IndexedRelationship {
    pub source: NodeId,
    pub relationship_key: String,
    pub target: Option<NodeId>,
    pub unresolved_target: Option<String>,
    pub provenance: RelationshipProvenance,
    pub inverse_label: Option<String>,
    pub source_kind: Option<String>,
    pub source_locator: Option<String>,
    pub parser: Option<String>,
    pub start_offset: Option<usize>,
    pub end_offset: Option<usize>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SearchHit {
    pub node_id: NodeId,
    pub title: String,
    pub source_kind: String,
    pub snippet: String,
    pub rank: f64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GraphNode {
    pub node_id: NodeId,
    pub title: String,
    pub is_orphan: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GraphEdge {
    pub source: NodeId,
    pub relationship_key: String,
    pub target: Option<NodeId>,
    pub unresolved_target: Option<String>,
    pub provenance: RelationshipProvenance,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GraphProjection {
    pub nodes: Vec<GraphNode>,
    pub edges: Vec<GraphEdge>,
}
