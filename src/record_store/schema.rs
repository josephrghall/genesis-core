pub(crate) const RECORD_SCHEMA_VERSION: u32 = 5;
pub(crate) const MINIMUM_RECORD_READER_VERSION: u32 = 1;

pub(crate) const RECORD_SCHEMA_V1: &str = r#"
CREATE TABLE store_meta (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    schema_version INTEGER NOT NULL CHECK (schema_version > 0),
    current_revision INTEGER NOT NULL CHECK (current_revision >= 0),
    minimum_reader_version INTEGER NOT NULL CHECK (minimum_reader_version > 0),
    created_at TEXT NOT NULL
);

CREATE TABLE nodes (
    node_rowid INTEGER PRIMARY KEY,
    node_uuid BLOB NOT NULL UNIQUE CHECK (length(node_uuid) = 16),
    lifecycle_state TEXT NOT NULL CHECK (lifecycle_state IN ('active', 'tombstoned')),
    created_revision INTEGER NOT NULL CHECK (created_revision > 0),
    modified_revision INTEGER NOT NULL CHECK (modified_revision >= created_revision)
);

CREATE TABLE node_properties (
    node_rowid INTEGER NOT NULL REFERENCES nodes(node_rowid) ON DELETE CASCADE,
    property_key TEXT NOT NULL CHECK (length(property_key) > 0),
    value_kind TEXT NOT NULL CHECK (value_kind IN (
        'null', 'boolean', 'integer', 'real', 'text', 'date', 'date_time', 'list', 'structured'
    )),
    text_value TEXT,
    integer_value INTEGER,
    real_value REAL,
    json_value TEXT,
    modified_revision INTEGER NOT NULL CHECK (modified_revision > 0),
    PRIMARY KEY (node_rowid, property_key),
    CHECK (
        (value_kind = 'null' AND text_value IS NULL AND integer_value IS NULL AND real_value IS NULL AND json_value IS NULL) OR
        (value_kind = 'boolean' AND integer_value IN (0, 1) AND text_value IS NULL AND real_value IS NULL AND json_value IS NULL) OR
        (value_kind = 'integer' AND integer_value IS NOT NULL AND text_value IS NULL AND real_value IS NULL AND json_value IS NULL) OR
        (value_kind = 'real' AND real_value IS NOT NULL AND text_value IS NULL AND integer_value IS NULL AND json_value IS NULL) OR
        (value_kind IN ('text', 'date', 'date_time') AND text_value IS NOT NULL AND integer_value IS NULL AND real_value IS NULL AND json_value IS NULL) OR
        (value_kind IN ('list', 'structured') AND json_value IS NOT NULL AND text_value IS NULL AND integer_value IS NULL AND real_value IS NULL)
    )
);

CREATE INDEX node_properties_key_idx ON node_properties(property_key);

CREATE TABLE relationships (
    relationship_rowid INTEGER PRIMARY KEY,
    source_node_rowid INTEGER NOT NULL REFERENCES nodes(node_rowid) ON DELETE CASCADE,
    relationship_key TEXT NOT NULL CHECK (length(relationship_key) > 0),
    target_node_rowid INTEGER NOT NULL REFERENCES nodes(node_rowid) ON DELETE CASCADE,
    created_revision INTEGER NOT NULL CHECK (created_revision > 0),
    modified_revision INTEGER NOT NULL CHECK (modified_revision >= created_revision),
    UNIQUE (source_node_rowid, relationship_key, target_node_rowid)
);

CREATE INDEX relationships_target_idx ON relationships(target_node_rowid, relationship_key);

CREATE TABLE property_definitions (
    key TEXT PRIMARY KEY CHECK (length(key) > 0),
    label TEXT NOT NULL,
    description TEXT,
    expected_value_kind TEXT CHECK (expected_value_kind IS NULL OR expected_value_kind IN (
        'null', 'boolean', 'integer', 'real', 'text', 'date', 'date_time', 'list', 'structured'
    )),
    lifecycle_state TEXT NOT NULL CHECK (lifecycle_state IN ('active', 'deprecated')),
    replacement_key TEXT REFERENCES property_definitions(key),
    origin TEXT NOT NULL,
    definition_version INTEGER NOT NULL CHECK (definition_version > 0),
    modified_revision INTEGER NOT NULL CHECK (modified_revision > 0)
);

CREATE TABLE relationship_definitions (
    key TEXT PRIMARY KEY CHECK (length(key) > 0),
    forward_label TEXT NOT NULL,
    inverse_label TEXT NOT NULL,
    description TEXT,
    lifecycle_state TEXT NOT NULL CHECK (lifecycle_state IN ('active', 'deprecated')),
    replacement_key TEXT REFERENCES relationship_definitions(key),
    origin TEXT NOT NULL,
    definition_version INTEGER NOT NULL CHECK (definition_version > 0),
    modified_revision INTEGER NOT NULL CHECK (modified_revision > 0)
);

CREATE TABLE record_changes (
    revision INTEGER NOT NULL CHECK (revision > 0),
    ordinal INTEGER NOT NULL CHECK (ordinal >= 0),
    change_kind TEXT NOT NULL,
    node_uuid BLOB CHECK (node_uuid IS NULL OR length(node_uuid) = 16),
    definition_key TEXT,
    descriptor_json TEXT,
    PRIMARY KEY (revision, ordinal)
);
"#;

// Schema v4 adds the canonical membership set of every issued Node UUID.
// It deliberately contains no Node lifecycle or reconstructable metadata.
pub(crate) const RECORD_SCHEMA_V4: &str = r#"
CREATE TABLE issued_node_ids (
    node_uuid BLOB PRIMARY KEY CHECK (length(node_uuid) = 16)
);

INSERT INTO issued_node_ids (node_uuid)
SELECT node_uuid FROM nodes;
"#;

// Relationship endpoints are permanent Genesis identities, not transient
// Node rowids. A purged Node removes owned state but cannot cascade-delete
// an independently surviving canonical Relationship.
pub(crate) const RECORD_SCHEMA_V5: &str = r#"
ALTER TABLE relationships RENAME TO relationships_v4;

CREATE TABLE relationships (
    relationship_rowid INTEGER PRIMARY KEY,
    source_uuid BLOB NOT NULL REFERENCES issued_node_ids(node_uuid)
        CHECK (length(source_uuid) = 16),
    relationship_key TEXT NOT NULL CHECK (length(relationship_key) > 0),
    target_uuid BLOB NOT NULL REFERENCES issued_node_ids(node_uuid)
        CHECK (length(target_uuid) = 16),
    created_revision INTEGER NOT NULL CHECK (created_revision > 0),
    modified_revision INTEGER NOT NULL CHECK (modified_revision >= created_revision),
    UNIQUE (source_uuid, relationship_key, target_uuid)
);

INSERT INTO relationships
    (relationship_rowid, source_uuid, relationship_key, target_uuid,
     created_revision, modified_revision)
SELECT r.relationship_rowid, s.node_uuid, r.relationship_key, t.node_uuid,
       r.created_revision, r.modified_revision
FROM relationships_v4 r
JOIN nodes s ON s.node_rowid=r.source_node_rowid
JOIN nodes t ON t.node_rowid=r.target_node_rowid;

DROP TABLE relationships_v4;
CREATE INDEX relationships_target_idx
    ON relationships(target_uuid, relationship_key);
"#;
