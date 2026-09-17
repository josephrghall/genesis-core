use serde::{Deserialize, Serialize};

use super::NodeId;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LifecycleState {
    Active,
    Tombstoned,
}

/// Minimal Core Node representation. Semantic fields belong to Properties,
/// Relationships, or capabilities rather than this struct.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Node {
    pub id: NodeId,
    pub lifecycle_state: LifecycleState,
}

impl Node {
    pub fn blank() -> Self {
        Self {
            id: NodeId::new(),
            lifecycle_state: LifecycleState::Active,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn blank_node_requires_no_title_type_path_or_content() {
        let node = Node::blank();
        assert_eq!(node.lifecycle_state, LifecycleState::Active);
        assert_eq!(
            serde_json::to_value(&node)
                .expect("serialize")
                .as_object()
                .unwrap()
                .len(),
            2
        );
    }
}
