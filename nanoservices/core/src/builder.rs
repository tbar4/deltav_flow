use std::collections::HashMap;
use std::sync::Arc;

use crate::dag::node::TaskNode;
use crate::dag::resolver::{self, DagError};
use crate::events::trigger::Trigger;
use crate::scheduler::runner::PipelineDef;
use crate::sources::traits::Source;
use crate::transforms::traits::Transform;
use crate::destinations::traits::Destination;

#[derive(Debug, thiserror::Error)]
pub enum BuildError {
    #[error("pipeline name is required")]
    NoName,
    #[error("trigger is required")]
    NoTrigger,
    #[error("at least one source is required")]
    NoSource,
    #[error("at least one destination is required")]
    NoDestination,
    #[error("DAG resolution failed: {0}")]
    DagError(#[from] DagError),
}

pub struct PipelineBuilder {
    name: Option<String>,
    trigger: Option<Trigger>,
    sources: Vec<(String, Arc<dyn Source>)>,
    transforms: Vec<(String, Arc<dyn Transform>)>,
    destinations: Vec<(String, Arc<dyn Destination>)>,
}

impl PipelineBuilder {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: Some(name.into()),
            trigger: None,
            sources: Vec::new(),
            transforms: Vec::new(),
            destinations: Vec::new(),
        }
    }

    pub fn trigger(mut self, trigger: Trigger) -> Self {
        self.trigger = Some(trigger);
        self
    }

    pub fn source(mut self, source: impl Source + 'static) -> Self {
        let name = source.name().to_string();
        self.sources.push((name, Arc::new(source)));
        self
    }

    pub fn transform(mut self, transform: impl Transform + 'static) -> Self {
        let name = transform.name().to_string();
        self.transforms.push((name, Arc::new(transform)));
        self
    }

    pub fn destination(mut self, destination: impl Destination + 'static) -> Self {
        let name = destination.name().to_string();
        self.destinations.push((name, Arc::new(destination)));
        self
    }

    pub fn build(self) -> Result<(Trigger, PipelineDef), BuildError> {
        let name = self.name.ok_or(BuildError::NoName)?;
        let trigger = self.trigger.ok_or(BuildError::NoTrigger)?;

        if self.sources.is_empty() {
            return Err(BuildError::NoSource);
        }
        if self.destinations.is_empty() {
            return Err(BuildError::NoDestination);
        }

        // Build TaskNodes from trait metadata
        let mut task_nodes = Vec::new();
        let mut source_map: HashMap<String, Arc<dyn Source>> = HashMap::new();
        let mut transform_map: HashMap<String, Arc<dyn Transform>> = HashMap::new();
        let mut dest_map: HashMap<String, Arc<dyn Destination>> = HashMap::new();

        for (task_name, src) in self.sources {
            task_nodes.push(TaskNode::source(&task_name, src.produces()));
            source_map.insert(task_name, src);
        }

        for (task_name, t) in self.transforms {
            let consumes: Vec<&str> = t.consumes().into_iter().collect();
            task_nodes.push(TaskNode::transform(&task_name, consumes, t.produces()));
            transform_map.insert(task_name, t);
        }

        for (task_name, dest) in self.destinations {
            task_nodes.push(TaskNode::destination(&task_name, dest.consumes()));
            dest_map.insert(task_name, dest);
        }

        let dag = resolver::resolve(task_nodes.clone())?;

        let node_map: HashMap<String, TaskNode> = task_nodes.into_iter()
            .map(|n| (n.name.clone(), n))
            .collect();

        Ok((trigger, PipelineDef {
            name,
            dag,
            nodes: node_map,
            sources: source_map,
            transforms: transform_map,
            destinations: dest_map,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use deltav_utils::{DeltavFlowResult, DeltavStream};
    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use std::time::Duration;

    struct FakeSource;

    #[async_trait::async_trait]
    impl Source for FakeSource {
        fn name(&self) -> &str { "fetch" }
        fn produces(&self) -> &str { "raw" }
        async fn extract(&self) -> DeltavFlowResult<DeltavStream> {
            let schema = Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int32, false),
            ]));
            let batch = RecordBatch::try_new(
                schema,
                vec![Arc::new(Int32Array::from(vec![1]))],
            ).unwrap();
            Ok(DeltavStream::new(vec![batch]))
        }
    }

    struct FakeDest;

    #[async_trait::async_trait]
    impl Destination for FakeDest {
        fn name(&self) -> &str { "save" }
        fn consumes(&self) -> &str { "raw" }
        async fn load(&self, _: DeltavStream) -> DeltavFlowResult<()> { Ok(()) }
    }

    #[test]
    fn builder_creates_pipeline_def() {
        let (trigger, def) = PipelineBuilder::new("test")
            .trigger(Trigger::Interval(Duration::from_secs(60)))
            .source(FakeSource)
            .destination(FakeDest)
            .build()
            .unwrap();

        assert_eq!(def.name, "test");
        assert!(matches!(trigger, Trigger::Interval(_)));
        assert_eq!(def.dag.order, vec!["fetch", "save"]);
    }

    #[test]
    fn builder_requires_source() {
        let result = PipelineBuilder::new("test")
            .trigger(Trigger::Interval(Duration::from_secs(60)))
            .destination(FakeDest)
            .build();

        assert!(matches!(result, Err(BuildError::NoSource)));
    }

    #[test]
    fn builder_requires_trigger() {
        let result = PipelineBuilder::new("test")
            .source(FakeSource)
            .destination(FakeDest)
            .build();

        assert!(matches!(result, Err(BuildError::NoTrigger)));
    }
}
