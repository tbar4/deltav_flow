use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{watch, Mutex};

use crate::events::bus::EventBus;
use crate::events::interval::spawn_interval_trigger;
use crate::events::trigger::Trigger;
use crate::scheduler::runner::{self, PipelineDef};
use crate::store::db::Store;

/// Top-level orchestrator. Users register pipelines and call run().
pub struct DeltavFlow {
    pipelines: Vec<(Trigger, PipelineDef)>,
    db_path: Option<String>,
}

impl DeltavFlow {
    pub fn new() -> Self {
        Self {
            pipelines: Vec::new(),
            db_path: None,
        }
    }

    /// Set the path for the SQLite database. Defaults to in-memory.
    pub fn db_path(mut self, path: impl Into<String>) -> Self {
        self.db_path = Some(path.into());
        self
    }

    /// Add a pipeline (trigger + definition).
    pub fn add_pipeline(mut self, trigger: Trigger, def: PipelineDef) -> Self {
        self.pipelines.push((trigger, def));
        self
    }

    /// Run the orchestrator until Ctrl-C.
    pub async fn run(self) -> deltav_utils::DeltavFlowResult<()> {
        let shutdown = tokio::signal::ctrl_c();
        self.run_with_shutdown(async { let _ = shutdown.await; }).await
    }

    /// Run with a custom shutdown signal (useful for testing).
    pub async fn run_with_shutdown<F: std::future::Future>(self, shutdown: F) -> deltav_utils::DeltavFlowResult<()> {
        // Initialize store
        let store = if let Some(path) = &self.db_path {
            Store::open(path).expect("failed to open database")
        } else {
            Store::in_memory().expect("failed to create in-memory database")
        };

        // Crash recovery
        let crashed = store.mark_inflight_as_crashed().unwrap_or(0);
        if crashed > 0 {
            tracing::warn!(count = crashed, "marked in-flight runs as crashed from previous session");
        }

        let store = Arc::new(Mutex::new(store));

        // Set up event bus
        let bus = EventBus::new(256);
        let (event_tx, event_rx) = bus.split();

        // Set up shutdown broadcast
        let (shutdown_tx, shutdown_rx) = watch::channel(false);

        // Spawn trigger producers
        let mut trigger_handles = Vec::new();
        let mut pipeline_defs = HashMap::new();

        for (trigger, def) in self.pipelines {
            let pipeline_name = def.name.clone();
            match &trigger {
                Trigger::Interval(duration) => {
                    let handle = spawn_interval_trigger(
                        pipeline_name.clone(),
                        *duration,
                        event_tx.clone(),
                    );
                    trigger_handles.push(handle);
                }
                Trigger::Webhook { .. } => {
                    tracing::warn!(pipeline = %pipeline_name, "webhook triggers not yet implemented");
                }
                Trigger::FileWatch { .. } => {
                    tracing::warn!(pipeline = %pipeline_name, "file watch triggers not yet implemented");
                }
            }
            pipeline_defs.insert(pipeline_name, def);
        }

        // Drop our copy of the sender so scheduler sees channel close on shutdown
        drop(event_tx);

        // Start scheduler
        let scheduler_handle = tokio::spawn(runner::run_scheduler(
            event_rx,
            pipeline_defs,
            store,
            shutdown_rx,
        ));

        // Wait for shutdown signal
        shutdown.await;

        // Broadcast shutdown
        let _ = shutdown_tx.send(true);

        // Abort trigger producers
        for handle in trigger_handles {
            handle.abort();
        }

        // Wait for scheduler to finish
        let _ = scheduler_handle.await;

        tracing::info!("deltav shutdown complete");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::builder::PipelineBuilder;
    use deltav_utils::{DeltavFlowResult, DeltavStream};
    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    struct CountSource {
        count: Arc<AtomicUsize>,
    }

    #[async_trait::async_trait]
    impl crate::sources::traits::Source for CountSource {
        fn name(&self) -> &str { "count_source" }
        fn produces(&self) -> &str { "counts" }
        async fn extract(&self) -> DeltavFlowResult<DeltavStream> {
            self.count.fetch_add(1, Ordering::SeqCst);
            let schema = Arc::new(Schema::new(vec![
                Field::new("n", DataType::Int32, false),
            ]));
            let batch = RecordBatch::try_new(
                schema,
                vec![Arc::new(Int32Array::from(vec![1]))],
            ).unwrap();
            Ok(DeltavStream::new(vec![batch]))
        }
    }

    struct NullDest;

    #[async_trait::async_trait]
    impl crate::destinations::traits::Destination for NullDest {
        fn name(&self) -> &str { "null" }
        fn consumes(&self) -> &str { "counts" }
        async fn load(&self, _: DeltavStream) -> DeltavFlowResult<()> { Ok(()) }
    }

    #[tokio::test]
    async fn engine_runs_and_shuts_down() {
        let extract_count = Arc::new(AtomicUsize::new(0));

        let (trigger, def) = PipelineBuilder::new("test")
            .trigger(Trigger::Interval(Duration::from_millis(50)))
            .source(CountSource { count: extract_count.clone() })
            .destination(NullDest)
            .build()
            .unwrap();

        let engine = DeltavFlow::new()
            .add_pipeline(trigger, def);

        // Run for 300ms then shutdown
        engine.run_with_shutdown(async {
            tokio::time::sleep(Duration::from_millis(300)).await;
        }).await.unwrap();

        // Should have extracted multiple times
        let count = extract_count.load(Ordering::SeqCst);
        assert!(count >= 2, "expected at least 2 extractions, got {count}");
    }
}
