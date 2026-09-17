use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::CoreError;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PropertyValueKind {
    Null,
    Boolean,
    Integer,
    Real,
    Text,
    Date,
    DateTime,
    List,
    Structured,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum PropertyValue {
    Null,
    Boolean(bool),
    Integer(i64),
    Real(f64),
    Text(String),
    Date(String),
    DateTime(String),
    List(Vec<PropertyValue>),
    Structured(Value),
}

impl PropertyValue {
    pub fn kind(&self) -> PropertyValueKind {
        match self {
            Self::Null => PropertyValueKind::Null,
            Self::Boolean(_) => PropertyValueKind::Boolean,
            Self::Integer(_) => PropertyValueKind::Integer,
            Self::Real(_) => PropertyValueKind::Real,
            Self::Text(_) => PropertyValueKind::Text,
            Self::Date(_) => PropertyValueKind::Date,
            Self::DateTime(_) => PropertyValueKind::DateTime,
            Self::List(_) => PropertyValueKind::List,
            Self::Structured(_) => PropertyValueKind::Structured,
        }
    }

    /// Stable UTF-8 JSON carrier for List and Structured values.
    pub fn canonical_json(&self) -> Option<String> {
        match self {
            Self::List(values) => serde_json::to_string(values).ok(),
            Self::Structured(value) => serde_json::to_string(value).ok(),
            _ => None,
        }
    }

    pub(crate) fn validate(&self) -> Result<(), CoreError> {
        match self {
            Self::Real(value) if !value.is_finite() => Err(CoreError::InvalidPropertyValue(
                "real values must be finite".into(),
            )),
            Self::Date(value) => chrono::NaiveDate::parse_from_str(value, "%Y-%m-%d")
                .map(|_| ())
                .map_err(|_| {
                    CoreError::InvalidPropertyValue(
                        "dates must use a valid YYYY-MM-DD representation".into(),
                    )
                }),
            Self::DateTime(value) => chrono::DateTime::parse_from_rfc3339(value)
                .map(|_| ())
                .map_err(|_| {
                    CoreError::InvalidPropertyValue(
                        "date-times must use RFC 3339 with an explicit offset".into(),
                    )
                }),
            Self::List(values) => values.iter().try_for_each(Self::validate),
            Self::Structured(value) if contains_non_finite_number(value) => {
                Err(CoreError::InvalidPropertyValue(
                    "structured JSON contains a non-finite number".into(),
                ))
            }
            _ => Ok(()),
        }
    }
}

fn contains_non_finite_number(value: &Value) -> bool {
    match value {
        Value::Array(values) => values.iter().any(contains_non_finite_number),
        Value::Object(values) => values.values().any(contains_non_finite_number),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn preserves_all_locked_property_value_kinds() {
        let values = [
            PropertyValue::Null,
            PropertyValue::Boolean(true),
            PropertyValue::Integer(4),
            PropertyValue::Real(4.5),
            PropertyValue::Text("Genesis".into()),
            PropertyValue::Date("2026-09-04".into()),
            PropertyValue::DateTime("2026-09-04T12:00:00-07:00".into()),
            PropertyValue::List(vec![PropertyValue::Integer(1)]),
            PropertyValue::Structured(serde_json::json!({"key": "value"})),
        ];

        let kinds = values.map(|value| value.kind());
        assert_eq!(
            kinds,
            [
                PropertyValueKind::Null,
                PropertyValueKind::Boolean,
                PropertyValueKind::Integer,
                PropertyValueKind::Real,
                PropertyValueKind::Text,
                PropertyValueKind::Date,
                PropertyValueKind::DateTime,
                PropertyValueKind::List,
                PropertyValueKind::Structured,
            ]
        );
    }

    #[test]
    fn list_and_structured_values_have_portable_json_carriers() {
        let list = PropertyValue::List(vec![PropertyValue::Integer(1)]);
        let structured = PropertyValue::Structured(serde_json::json!({"b": 2, "a": 1}));

        assert_eq!(
            list.canonical_json().expect("list JSON"),
            r#"[{"kind":"integer","value":1}]"#
        );
        assert_eq!(
            structured.canonical_json().expect("structured JSON"),
            r#"{"a":1,"b":2}"#
        );
    }
}
