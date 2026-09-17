use thiserror::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CoreErrorCategory {
    NotFound,
    AlreadyExists,
    InvalidId,
    InvalidProperty,
    InvalidPropertyValue,
    InvalidRelationship,
    DefinitionNotFound,
    Conflict,
    StorageUnavailable,
    IntegrityFailure,
    PermissionDenied,
    OperationFailed,
    PostCommitFailure,
}

#[derive(Debug, Error)]
pub enum CoreError {
    #[error("Core filesystem operation failed: {0}")]
    Io(#[from] std::io::Error),

    #[error("Core database operation failed: {0}")]
    Database(#[from] rusqlite::Error),

    #[error("invalid Genesis Node ID: {0}")]
    InvalidNodeId(String),

    #[error("{entity} not found: {identifier}")]
    NotFound {
        entity: &'static str,
        identifier: String,
    },

    #[error("{entity} already exists: {identifier}")]
    AlreadyExists {
        entity: &'static str,
        identifier: String,
    },

    #[error("invalid property key: {0}")]
    InvalidProperty(String),

    #[error("invalid property value: {0}")]
    InvalidPropertyValue(String),

    #[error("invalid relationship: {0}")]
    InvalidRelationship(String),

    #[error("definition not found: {0}")]
    DefinitionNotFound(String),

    #[error("operation conflicts with current state: {0}")]
    Conflict(String),

    #[error("canonical revision {revision} committed, but {follow_up} failed: {reason}")]
    PostCommitFailure {
        revision: u64,
        follow_up: &'static str,
        reason: String,
    },

    #[error("Index is unavailable: {0}")]
    IndexUnavailable(String),

    #[error(
        "{store} schema version {found} is unsupported; this reader supports version {supported}"
    )]
    UnsupportedSchema {
        store: &'static str,
        found: u32,
        supported: u32,
    },

    #[error("{store} failed validation: {reason}")]
    CorruptStore { store: &'static str, reason: String },

    #[error("protected Store is unavailable: {0}")]
    ProtectedStoreUnavailable(String),

    #[error("{0} lock is poisoned")]
    LockPoisoned(&'static str),

    #[error("failed to close {store}: {source}")]
    CloseStore {
        store: &'static str,
        source: rusqlite::Error,
    },
}

impl CoreError {
    pub fn category(&self) -> CoreErrorCategory {
        match self {
            Self::NotFound { .. } => CoreErrorCategory::NotFound,
            Self::AlreadyExists { .. } => CoreErrorCategory::AlreadyExists,
            Self::InvalidNodeId(_) => CoreErrorCategory::InvalidId,
            Self::InvalidProperty(_) => CoreErrorCategory::InvalidProperty,
            Self::InvalidPropertyValue(_) => CoreErrorCategory::InvalidPropertyValue,
            Self::InvalidRelationship(_) => CoreErrorCategory::InvalidRelationship,
            Self::DefinitionNotFound(_) => CoreErrorCategory::DefinitionNotFound,
            Self::Conflict(_) => CoreErrorCategory::Conflict,
            Self::PostCommitFailure { .. } => CoreErrorCategory::PostCommitFailure,
            Self::Io(error) if error.kind() == std::io::ErrorKind::PermissionDenied => {
                CoreErrorCategory::PermissionDenied
            }
            Self::Io(_)
            | Self::LockPoisoned(_)
            | Self::CloseStore { .. }
            | Self::IndexUnavailable(_) => CoreErrorCategory::StorageUnavailable,
            Self::UnsupportedSchema { .. } | Self::CorruptStore { .. } => {
                CoreErrorCategory::IntegrityFailure
            }
            Self::ProtectedStoreUnavailable(_) => CoreErrorCategory::StorageUnavailable,
            Self::Database(_) => CoreErrorCategory::OperationFailed,
        }
    }
}
