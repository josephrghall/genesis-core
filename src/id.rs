use std::{fmt, str::FromStr};

use serde::{Deserialize, Deserializer, Serialize, Serializer};
use uuid::Uuid;

use super::CoreError;

/// Permanent Genesis Node identity.
///
/// The inner UUID remains private so callers cannot bypass UUIDv7/nil checks.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct NodeId(Uuid);

impl NodeId {
    pub fn new() -> Self {
        Self(Uuid::now_v7())
    }

    pub fn from_uuid(uuid: Uuid) -> Result<Self, CoreError> {
        if uuid.is_nil() {
            return Err(CoreError::InvalidNodeId("nil UUID is not allowed".into()));
        }
        if uuid.get_version_num() != 7 {
            return Err(CoreError::InvalidNodeId(format!(
                "expected UUIDv7, received UUIDv{}",
                uuid.get_version_num()
            )));
        }
        Ok(Self(uuid))
    }

    pub fn from_bytes(bytes: [u8; 16]) -> Result<Self, CoreError> {
        Self::from_uuid(Uuid::from_bytes(bytes))
    }

    pub fn as_bytes(&self) -> &[u8; 16] {
        self.0.as_bytes()
    }

    pub fn into_bytes(self) -> [u8; 16] {
        self.0.into_bytes()
    }
}

impl Default for NodeId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for NodeId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}", self.0.hyphenated())
    }
}

impl FromStr for NodeId {
    type Err = CoreError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let uuid =
            Uuid::parse_str(value).map_err(|error| CoreError::InvalidNodeId(error.to_string()))?;
        Self::from_uuid(uuid)
    }
}

impl Serialize for NodeId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for NodeId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        value.parse().map_err(serde::de::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generated_ids_are_non_nil_uuid_v7_and_round_trip() {
        let id = NodeId::new();
        assert_ne!(id.as_bytes(), &[0; 16]);
        assert_eq!(id.to_string().parse::<NodeId>().expect("parse ID"), id);
        assert_eq!(id.to_string(), id.to_string().to_lowercase());
    }

    #[test]
    fn rejects_malformed_nil_and_non_v7_ids() {
        assert!("not-a-uuid".parse::<NodeId>().is_err());
        assert!("00000000-0000-0000-0000-000000000000"
            .parse::<NodeId>()
            .is_err());
        assert!("550e8400-e29b-41d4-a716-446655440000"
            .parse::<NodeId>()
            .is_err());
    }

    #[test]
    fn serde_uses_standard_lowercase_hyphenated_text() {
        let id = NodeId::new();
        let json = serde_json::to_string(&id).expect("serialize ID");
        assert_eq!(json, format!("\"{id}\""));
        assert_eq!(
            serde_json::from_str::<NodeId>(&json).expect("deserialize ID"),
            id
        );
    }
}
