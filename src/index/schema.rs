pub(crate) const INDEX_SCHEMA_VERSION: u32 = 4;
pub(crate) const MINIMUM_INDEX_READER_VERSION: u32 = 1;

// Phase 1 intentionally establishes only Index identity/version/checkpoint
// state. Derived projections are added in Phase 3, after the Core mutation API
// exists and can provide revisioned canonical input.
pub(crate) const INDEX_SCHEMA_V1: &str = r#"
CREATE TABLE index_meta (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    schema_version INTEGER NOT NULL CHECK (schema_version > 0),
    minimum_reader_version INTEGER NOT NULL CHECK (minimum_reader_version > 0),
    applied_record_revision INTEGER NOT NULL CHECK (applied_record_revision >= 0),
    created_at TEXT NOT NULL
);
"#;

pub(crate) const INDEX_SCHEMA_V2: &str = r#"
CREATE TABLE indexed_nodes (
    node_uuid BLOB PRIMARY KEY CHECK (length(node_uuid) = 16),
    lifecycle_state TEXT NOT NULL CHECK (lifecycle_state IN ('active', 'tombstoned')),
    modified_revision INTEGER NOT NULL CHECK (modified_revision > 0)
);

CREATE TABLE indexed_properties (
    node_uuid BLOB NOT NULL REFERENCES indexed_nodes(node_uuid) ON DELETE CASCADE,
    property_key TEXT NOT NULL,
    value_kind TEXT NOT NULL CHECK (value_kind IN (
        'null', 'boolean', 'integer', 'real', 'text', 'date', 'date_time', 'list', 'structured'
    )),
    text_value TEXT,
    integer_value INTEGER,
    real_value REAL,
    json_value TEXT,
    modified_revision INTEGER NOT NULL CHECK (modified_revision > 0),
    PRIMARY KEY (node_uuid, property_key)
);

CREATE INDEX indexed_properties_key_idx ON indexed_properties(property_key, value_kind);

CREATE TABLE indexed_relationships (
    source_uuid BLOB NOT NULL REFERENCES indexed_nodes(node_uuid) ON DELETE CASCADE,
    relationship_key TEXT NOT NULL,
    target_uuid BLOB NOT NULL REFERENCES indexed_nodes(node_uuid) ON DELETE CASCADE,
    modified_revision INTEGER NOT NULL CHECK (modified_revision > 0),
    PRIMARY KEY (source_uuid, relationship_key, target_uuid)
);

CREATE INDEX indexed_relationships_target_idx
    ON indexed_relationships(target_uuid, relationship_key, source_uuid);

CREATE TABLE indexed_property_definitions (
    key TEXT PRIMARY KEY,
    label TEXT NOT NULL,
    description TEXT,
    expected_value_kind TEXT,
    lifecycle_state TEXT NOT NULL CHECK (lifecycle_state IN ('active', 'deprecated')),
    replacement_key TEXT,
    origin TEXT NOT NULL,
    definition_version INTEGER NOT NULL CHECK (definition_version > 0),
    modified_revision INTEGER NOT NULL CHECK (modified_revision > 0)
);

CREATE TABLE indexed_relationship_definitions (
    key TEXT PRIMARY KEY,
    forward_label TEXT NOT NULL,
    inverse_label TEXT,
    description TEXT,
    lifecycle_state TEXT NOT NULL CHECK (lifecycle_state IN ('active', 'deprecated')),
    replacement_key TEXT,
    origin TEXT NOT NULL,
    definition_version INTEGER NOT NULL CHECK (definition_version > 0),
    modified_revision INTEGER NOT NULL CHECK (modified_revision > 0)
);
"#;

pub(crate) const INDEX_SCHEMA_V3: &str = r#"
CREATE TABLE indexed_search_documents (
    document_key TEXT PRIMARY KEY,
    node_uuid BLOB NOT NULL REFERENCES indexed_nodes(node_uuid) ON DELETE CASCADE,
    source_kind TEXT NOT NULL,
    title TEXT NOT NULL,
    body TEXT NOT NULL,
    metadata TEXT NOT NULL,
    source_locator TEXT,
    source_fingerprint TEXT NOT NULL
);

CREATE INDEX indexed_search_documents_node_idx
    ON indexed_search_documents(node_uuid, source_kind);

CREATE VIRTUAL TABLE indexed_search_fts USING fts5(
    document_key UNINDEXED,
    node_uuid UNINDEXED,
    title,
    body,
    metadata,
    tokenize = 'unicode61 remove_diacritics 2'
);

CREATE TABLE indexed_derived_relationships (
    source_uuid BLOB NOT NULL REFERENCES indexed_nodes(node_uuid) ON DELETE CASCADE,
    relationship_key TEXT NOT NULL,
    target_uuid BLOB REFERENCES indexed_nodes(node_uuid) ON DELETE SET NULL,
    target_text TEXT NOT NULL,
    origin TEXT NOT NULL CHECK (origin = 'content_derived'),
    source_kind TEXT NOT NULL,
    source_locator TEXT,
    parser TEXT NOT NULL,
    start_offset INTEGER NOT NULL CHECK (start_offset >= 0),
    end_offset INTEGER NOT NULL CHECK (end_offset >= start_offset),
    PRIMARY KEY (
        source_uuid,
        relationship_key,
        source_kind,
        parser,
        start_offset,
        end_offset
    )
);

CREATE INDEX indexed_derived_relationships_target_idx
    ON indexed_derived_relationships(target_uuid, relationship_key, source_uuid);
"#;

// Canonical Relationships may reference retired IDs; this derived projection
// cannot force them to disappear when an indexed Node is removed.
pub(crate) const INDEX_SCHEMA_V4: &str = r#"
ALTER TABLE indexed_relationships RENAME TO indexed_relationships_v3;
CREATE TABLE indexed_relationships (
    source_uuid BLOB NOT NULL CHECK (length(source_uuid) = 16),
    relationship_key TEXT NOT NULL,
    target_uuid BLOB NOT NULL CHECK (length(target_uuid) = 16),
    modified_revision INTEGER NOT NULL CHECK (modified_revision > 0),
    PRIMARY KEY (source_uuid, relationship_key, target_uuid)
);
INSERT INTO indexed_relationships
    (source_uuid, relationship_key, target_uuid, modified_revision)
SELECT source_uuid, relationship_key, target_uuid, modified_revision
FROM indexed_relationships_v3;
DROP TABLE indexed_relationships_v3;
CREATE INDEX indexed_relationships_target_idx
    ON indexed_relationships(target_uuid, relationship_key, source_uuid);
"#;
