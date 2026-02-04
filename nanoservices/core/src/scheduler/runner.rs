use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex};
use uuid::Uuid;

use deltav_utils::{DeltavFlowResult, DeltavStream};
use crate::dag::node::TaskNode;
use crate::dag::resolver::ResolvedDag;
use crate::events::trigger::TriggerEvent;
use crate::scheduler::state::{RunState, RunStatus, TaskStatus};
use crate::sources::traits::Source;
use crate::transforms::traits::Transform;
use crate::destinations::traits::Destination;
use crate::store::db::Store;

/// A registered pipeline with its resolved DAG and task implementations.
pub struct PipelineDef {
    pub name: String,
    pub dag: ResolvedDag,
    pub nodes: HashMap<String, TaskNode>,
    pub sources: HashMap<String, Arc<dyn Source>>,
    pub transforms: HashMap<String, Arc<dyn Transform>>,
    pub destinations: HashMap<String, Arc<dyn Destination>>,
}

/// Result of a single task execution, sent back to the scheduler.
struct TaskResult {
    run_id: String,
    task_name: String,
    produced_name: Option<String>,
    result: DeltavFlowResult<Option<DeltavStream>>,
    duration_ms: u64,
}

/// The scheduler loop. Receives trigger events and orchestrates pipeline runs.
pub async fn run_scheduler(
    mut event_rx: mpsc::Receiver<TriggerEvent>,
    pipelines: HashMap<String, PipelineDef>,
    store: Arc<Mutex<Store>>,
    mut shutdown_rx: tokio::sync::watch::Receiver<bool>,
) {
    let (result_tx, mut result_rx) = mpsc::channel::<TaskResult>(256);
    let mut active_runs: HashMap<String, RunState> = HashMap::new();

    loop {
        tokio::select! {
            // New trigger event
            Some(event) = event_rx.recv() => {
                let Some(pipeline) = pipelines.get(&event.pipeline) else {
                    tracing::warn!(pipeline = %event.pipeline, "received trigger for unknown pipeline");
                    continue;
                };

                let run_id = Uuid::new_v4().to_string();
                let now = chrono_now();

                // Persist run start
                {
                    let store = store.lock().await;
                    let trigger_type = format!("{:?}", event.trigger);
                    let _ = store.insert_pipeline_run(&run_id, &event.pipeline, &trigger_type, &now);
                }

                // Build consumes map from DAG nodes
                let task_consumes: HashMap<String, HashSet<String>> = pipeline.nodes.iter()
                    .map(|(name, node)| (name.clone(), node.consumes.clone()))
                    .collect();

                let mut run_state = RunState::new(run_id.clone(), event.pipeline.clone(), task_consumes);

                // Dispatch ready (root) tasks
                let ready = run_state.ready_tasks();
                for task_name in &ready {
                    run_state.task_statuses.insert(task_name.clone(), TaskStatus::Running);
                    dispatch_task(
                        &run_id,
                        task_name,
                        pipeline,
                        &run_state,
                        result_tx.clone(),
                    );
                }

                active_runs.insert(run_id, run_state);
            }

            // Task completed
            Some(task_result) = result_rx.recv() => {
                let Some(run_state) = active_runs.get_mut(&task_result.run_id) else {
                    continue;
                };

                let now = chrono_now();
                let pipeline_name = run_state.pipeline.clone();
                let Some(pipeline) = pipelines.get(&pipeline_name) else { continue; };

                match task_result.result {
                    Ok(data) => {
                        // Persist task success
                        {
                            let store = store.lock().await;
                            let _ = store.insert_task_run(
                                &Uuid::new_v4().to_string(),
                                &task_result.run_id,
                                &task_result.task_name,
                                "completed",
                                &now, Some(&now),
                                Some(task_result.duration_ms as i64),
                                None,
                            );
                        }

                        let newly_ready = run_state.task_completed(
                            &task_result.task_name,
                            task_result.produced_name.as_deref(),
                            data,
                        );

                        // Dispatch newly ready tasks
                        for task_name in &newly_ready {
                            run_state.task_statuses.insert(task_name.clone(), TaskStatus::Running);
                            dispatch_task(
                                &task_result.run_id,
                                task_name,
                                pipeline,
                                run_state,
                                result_tx.clone(),
                            );
                        }

                        // Record metrics
                        crate::metrics::inc_run(&pipeline_name);
                    }
                    Err(e) => {
                        tracing::error!(
                            run = %task_result.run_id,
                            task = %task_result.task_name,
                            error = %e,
                            "task failed"
                        );

                        // Persist task failure
                        {
                            let store = store.lock().await;
                            let _ = store.insert_task_run(
                                &Uuid::new_v4().to_string(),
                                &task_result.run_id,
                                &task_result.task_name,
                                "failed",
                                &now, Some(&now),
                                Some(task_result.duration_ms as i64),
                                Some(&e.to_string()),
                            );
                        }

                        run_state.task_failed(&task_result.task_name);
                        crate::metrics::inc_failure(&pipeline_name);
                    }
                }

                // If run is complete, flush to store and remove from active
                if matches!(run_state.status, RunStatus::Completed | RunStatus::Failed) {
                    let duration = run_state.started_at.elapsed().as_millis() as i64;
                    let status = match run_state.status {
                        RunStatus::Completed => "completed",
                        RunStatus::Failed => "failed",
                        _ => "unknown",
                    };
                    {
                        let store = store.lock().await;
                        let _ = store.complete_pipeline_run(
                            &task_result.run_id, status, &now, duration,
                        );
                    }

                    crate::metrics::observe_duration(&pipeline_name, duration as f64);
                    tracing::info!(
                        run = %task_result.run_id,
                        pipeline = %pipeline_name,
                        status = status,
                        duration_ms = duration,
                        "pipeline run finished"
                    );

                    active_runs.remove(&task_result.run_id);
                }
            }

            // Shutdown signal
            _ = shutdown_rx.changed() => {
                tracing::info!("scheduler shutting down");
                break;
            }
        }
    }
}

/// Dispatch a single task for async execution.
fn dispatch_task(
    run_id: &str,
    task_name: &str,
    pipeline: &PipelineDef,
    run_state: &RunState,
    result_tx: mpsc::Sender<TaskResult>,
) {
    let run_id = run_id.to_string();
    let task_name = task_name.to_string();
    let node = pipeline.nodes.get(&task_name).cloned();

    // Gather input data for this task
    let inputs: HashMap<String, DeltavStream> = if let Some(ref node) = node {
        node.consumes.iter()
            .filter_map(|name| run_state.data.get(name).map(|d| (name.clone(), d.clone())))
            .collect()
    } else {
        HashMap::new()
    };

    // Determine task type and dispatch
    if let Some(source) = pipeline.sources.get(&task_name) {
        let source = source.clone();
        let produced_name = node.and_then(|n| n.produces.clone());
        tokio::spawn(async move {
            let start = std::time::Instant::now();
            let result = source.extract().await.map(Some);
            let duration_ms = start.elapsed().as_millis() as u64;
            let _ = result_tx.send(TaskResult {
                run_id, task_name, produced_name, result, duration_ms,
            }).await;
        });
    } else if let Some(transform) = pipeline.transforms.get(&task_name) {
        let transform = transform.clone();
        let produced_name = node.and_then(|n| n.produces.clone());
        tokio::spawn(async move {
            let start = std::time::Instant::now();
            let result = transform.transform(inputs).await.map(Some);
            let duration_ms = start.elapsed().as_millis() as u64;
            let _ = result_tx.send(TaskResult {
                run_id, task_name, produced_name, result, duration_ms,
            }).await;
        });
    } else if let Some(destination) = pipeline.destinations.get(&task_name) {
        let destination = destination.clone();
        let input = inputs.into_values().next();
        tokio::spawn(async move {
            let start = std::time::Instant::now();
            let result = if let Some(stream) = input {
                destination.load(stream).await.map(|_| None)
            } else {
                Ok(None)
            };
            let duration_ms = start.elapsed().as_millis() as u64;
            let _ = result_tx.send(TaskResult {
                run_id, task_name, produced_name: None, result, duration_ms,
            }).await;
        });
    }
}

fn chrono_now() -> String {
    // Simple ISO-8601 timestamp using std
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    format!("{now}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dag::node::TaskNode;
    use crate::dag::resolver;
    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::Duration;

    struct TestSource {
        name: String,
        produces: String,
    }

    #[async_trait::async_trait]
    impl Source for TestSource {
        fn name(&self) -> &str { &self.name }
        fn produces(&self) -> &str { &self.produces }
        async fn extract(&self) -> DeltavFlowResult<DeltavStream> {
            let schema = Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int32, false),
            ]));
            let batch = RecordBatch::try_new(
                schema,
                vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
            ).unwrap();
            Ok(DeltavStream::new(vec![batch]))
        }
    }

    struct TestDestination {
        name: String,
        consumes: String,
        loaded: Arc<AtomicBool>,
    }

    #[async_trait::async_trait]
    impl Destination for TestDestination {
        fn name(&self) -> &str { &self.name }
        fn consumes(&self) -> &str { &self.consumes }
        async fn load(&self, _stream: DeltavStream) -> DeltavFlowResult<()> {
            self.loaded.store(true, Ordering::SeqCst);
            Ok(())
        }
    }

    #[tokio::test]
    async fn scheduler_runs_simple_pipeline() {
        let loaded = Arc::new(AtomicBool::new(false));

        // Build pipeline def
        let nodes = vec![
            TaskNode::source("fetch", "raw"),
            TaskNode::destination("save", "raw"),
        ];
        let dag = resolver::resolve(nodes.clone()).unwrap();
        let node_map: HashMap<String, TaskNode> = nodes.into_iter()
            .map(|n| (n.name.clone(), n))
            .collect();

        let mut sources: HashMap<String, Arc<dyn Source>> = HashMap::new();
        sources.insert("fetch".to_string(), Arc::new(TestSource {
            name: "fetch".to_string(),
            produces: "raw".to_string(),
        }));

        let mut destinations: HashMap<String, Arc<dyn Destination>> = HashMap::new();
        destinations.insert("save".to_string(), Arc::new(TestDestination {
            name: "save".to_string(),
            consumes: "raw".to_string(),
            loaded: loaded.clone(),
        }));

        let pipeline_def = PipelineDef {
            name: "test".to_string(),
            dag,
            nodes: node_map,
            sources,
            transforms: HashMap::new(),
            destinations,
        };

        let mut pipelines = HashMap::new();
        pipelines.insert("test".to_string(), pipeline_def);

        let store = Arc::new(Mutex::new(Store::in_memory().unwrap()));
        let (event_tx, event_rx) = mpsc::channel(16);
        let (_shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

        // Start scheduler in background
        let scheduler_handle = tokio::spawn(run_scheduler(event_rx, pipelines, store.clone(), shutdown_rx));

        // Send trigger event
        event_tx.send(TriggerEvent {
            pipeline: "test".to_string(),
            trigger: crate::events::trigger::Trigger::Interval(Duration::from_secs(60)),
        }).await.unwrap();

        // Wait for pipeline to complete
        tokio::time::sleep(Duration::from_millis(200)).await;

        // Verify destination was loaded
        assert!(loaded.load(Ordering::SeqCst), "destination should have been loaded");

        // Verify run was persisted
        {
            let store = store.lock().await;
            let runs = store.recent_runs(10).unwrap();
            assert_eq!(runs.len(), 1);
            assert_eq!(runs[0].status, "completed");
        }

        // Shutdown
        _shutdown_tx.send(true).unwrap();
        let _ = tokio::time::timeout(Duration::from_secs(1), scheduler_handle).await;
    }
}
