use std::collections::{HashMap, HashSet, VecDeque};
use crate::dag::node::TaskNode;

/// Errors that can occur during DAG resolution.
#[derive(Debug, thiserror::Error)]
pub enum DagError {
    #[error("No producer found for data dependency: {0}")]
    MissingProducer(String),

    #[error("Multiple producers for data dependency '{0}': {1:?}")]
    AmbiguousProducer(String, Vec<String>),

    #[error("Cycle detected in DAG involving tasks: {0:?}")]
    CycleDetected(Vec<String>),
}

/// A resolved DAG with execution order.
#[derive(Debug)]
pub struct ResolvedDag {
    /// Tasks in topological order (safe to execute in this sequence).
    pub order: Vec<String>,
    /// Map from task name to its dependencies (task names it must wait for).
    pub dependencies: HashMap<String, HashSet<String>>,
    /// Map from task name to tasks that depend on it.
    pub dependents: HashMap<String, HashSet<String>>,
}

/// Resolve a set of TaskNodes into an execution DAG based on data contracts.
pub fn resolve(nodes: Vec<TaskNode>) -> Result<ResolvedDag, DagError> {
    // Build producer index: data_name -> task_name
    let mut producers: HashMap<String, String> = HashMap::new();
    for node in &nodes {
        if let Some(ref data_name) = node.produces {
            if let Some(existing) = producers.get(data_name) {
                return Err(DagError::AmbiguousProducer(
                    data_name.clone(),
                    vec![existing.clone(), node.name.clone()],
                ));
            }
            producers.insert(data_name.clone(), node.name.clone());
        }
    }

    // Build dependency graph: task_name -> set of task_names it depends on
    let mut dependencies: HashMap<String, HashSet<String>> = HashMap::new();
    let mut dependents: HashMap<String, HashSet<String>> = HashMap::new();

    for node in &nodes {
        let deps: HashSet<String> = node
            .consumes
            .iter()
            .map(|data_name| {
                producers
                    .get(data_name)
                    .cloned()
                    .ok_or_else(|| DagError::MissingProducer(data_name.clone()))
            })
            .collect::<Result<HashSet<_>, _>>()?;

        for dep in &deps {
            dependents
                .entry(dep.clone())
                .or_default()
                .insert(node.name.clone());
        }

        dependencies.insert(node.name.clone(), deps);
    }

    // Ensure all nodes are in the maps
    for node in &nodes {
        dependencies.entry(node.name.clone()).or_default();
        dependents.entry(node.name.clone()).or_default();
    }

    // Topological sort (Kahn's algorithm)
    let mut in_degree: HashMap<String, usize> = HashMap::new();
    for node in &nodes {
        in_degree.insert(node.name.clone(), dependencies[&node.name].len());
    }

    let mut queue: VecDeque<String> = in_degree
        .iter()
        .filter(|(_, deg)| **deg == 0)
        .map(|(name, _)| name.clone())
        .collect();

    let mut order: Vec<String> = Vec::new();

    while let Some(task) = queue.pop_front() {
        order.push(task.clone());
        for dependent in dependents.get(&task).unwrap_or(&HashSet::new()) {
            let deg = in_degree.get_mut(dependent).unwrap();
            *deg -= 1;
            if *deg == 0 {
                queue.push_back(dependent.clone());
            }
        }
    }

    if order.len() != nodes.len() {
        let remaining: Vec<String> = in_degree
            .into_iter()
            .filter(|(_, deg)| *deg > 0)
            .map(|(name, _)| name)
            .collect();
        return Err(DagError::CycleDetected(remaining));
    }

    Ok(ResolvedDag {
        order,
        dependencies,
        dependents,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dag::node::TaskNode;

    #[test]
    fn linear_pipeline_resolves() {
        let nodes = vec![
            TaskNode::source("fetch", "raw"),
            TaskNode::transform("clean", vec!["raw"], "clean"),
            TaskNode::destination("save", "clean"),
        ];

        let dag = resolve(nodes).unwrap();
        assert_eq!(dag.order, vec!["fetch", "clean", "save"]);
        assert!(dag.dependencies["fetch"].is_empty());
        assert!(dag.dependencies["clean"].contains("fetch"));
        assert!(dag.dependencies["save"].contains("clean"));
    }

    #[test]
    fn fan_out_resolves() {
        // One source feeds two destinations
        let nodes = vec![
            TaskNode::source("fetch", "raw"),
            TaskNode::destination("save_a", "raw"),
            TaskNode::destination("save_b", "raw"),
        ];

        let dag = resolve(nodes).unwrap();
        assert_eq!(dag.order[0], "fetch");
        assert!(dag.dependencies["save_a"].contains("fetch"));
        assert!(dag.dependencies["save_b"].contains("fetch"));
    }

    #[test]
    fn fan_in_resolves() {
        let nodes = vec![
            TaskNode::source("fetch_a", "data_a"),
            TaskNode::source("fetch_b", "data_b"),
            TaskNode::transform("merge", vec!["data_a", "data_b"], "merged"),
            TaskNode::destination("save", "merged"),
        ];

        let dag = resolve(nodes).unwrap();
        assert!(dag.dependencies["merge"].contains("fetch_a"));
        assert!(dag.dependencies["merge"].contains("fetch_b"));
        assert!(dag.dependencies["save"].contains("merge"));
    }

    #[test]
    fn missing_producer_errors() {
        let nodes = vec![
            TaskNode::destination("save", "nonexistent"),
        ];

        let err = resolve(nodes).unwrap_err();
        assert!(matches!(err, DagError::MissingProducer(ref s) if s == "nonexistent"));
    }

    #[test]
    fn ambiguous_producer_errors() {
        let nodes = vec![
            TaskNode::source("fetch_a", "raw"),
            TaskNode::source("fetch_b", "raw"),
            TaskNode::destination("save", "raw"),
        ];

        let err = resolve(nodes).unwrap_err();
        assert!(matches!(err, DagError::AmbiguousProducer(ref s, _) if s == "raw"));
    }

    #[test]
    fn diamond_dag_resolves() {
        //    fetch
        //    /    \
        // clean_a  clean_b
        //    \    /
        //    merge
        //      |
        //    save
        let nodes = vec![
            TaskNode::source("fetch", "raw"),
            TaskNode::transform("clean_a", vec!["raw"], "a_out"),
            TaskNode::transform("clean_b", vec!["raw"], "b_out"),
            TaskNode::transform("merge", vec!["a_out", "b_out"], "merged"),
            TaskNode::destination("save", "merged"),
        ];

        let dag = resolve(nodes).unwrap();
        // fetch must be first, save must be last
        assert_eq!(dag.order[0], "fetch");
        assert_eq!(*dag.order.last().unwrap(), "save");
        // merge must come after both clean tasks
        let merge_pos = dag.order.iter().position(|n| n == "merge").unwrap();
        let a_pos = dag.order.iter().position(|n| n == "clean_a").unwrap();
        let b_pos = dag.order.iter().position(|n| n == "clean_b").unwrap();
        assert!(merge_pos > a_pos);
        assert!(merge_pos > b_pos);
    }
}
