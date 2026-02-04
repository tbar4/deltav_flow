use std::collections::HashSet;

/// Represents a single task in the DAG with its data contracts.
#[derive(Debug, Clone)]
pub struct TaskNode {
    pub name: String,
    pub produces: Option<String>,
    pub consumes: HashSet<String>,
}

impl TaskNode {
    pub fn source(name: impl Into<String>, produces: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            produces: Some(produces.into()),
            consumes: HashSet::new(),
        }
    }

    pub fn transform(
        name: impl Into<String>,
        consumes: Vec<&str>,
        produces: impl Into<String>,
    ) -> Self {
        Self {
            name: name.into(),
            produces: Some(produces.into()),
            consumes: consumes.into_iter().map(String::from).collect(),
        }
    }

    pub fn destination(name: impl Into<String>, consumes: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            produces: None,
            consumes: [consumes.into()].into_iter().collect(),
        }
    }

    pub fn is_root(&self) -> bool {
        self.consumes.is_empty()
    }

    pub fn is_leaf(&self) -> bool {
        self.produces.is_none()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn source_node_is_root() {
        let node = TaskNode::source("fetch", "raw_data");
        assert!(node.is_root());
        assert!(!node.is_leaf());
        assert_eq!(node.produces, Some("raw_data".to_string()));
    }

    #[test]
    fn destination_node_is_leaf() {
        let node = TaskNode::destination("save", "clean_data");
        assert!(!node.is_root());
        assert!(node.is_leaf());
        assert!(node.consumes.contains("clean_data"));
    }

    #[test]
    fn transform_node_is_middle() {
        let node = TaskNode::transform("clean", vec!["raw_data"], "clean_data");
        assert!(!node.is_root());
        assert!(!node.is_leaf());
    }
}
