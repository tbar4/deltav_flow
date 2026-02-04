
use deltav_utils::DeltavFlowResult;
use tokio::time::{Instant, Duration};
use tokio::sync::watch;
use tokio::signal;
use uuid::Uuid;
use std::pin::Pin;
use std::future::Future;
use tracing::{info, error, debug};

use super::sources::Source;
use super::destinations::Destination;
use crate::metrics;

/// Pipeline module: defines the `Pipeline` type and a simple `Scheduler`.
///
/// A `Pipeline` owns a `Source` and a `Destination` and knows how to extract data
/// from the source and load it into the destination on a fixed interval. Each
/// pipeline may register an async cleanup hook to run when the scheduler shuts down.
pub mod helpers;

type CleanupFn = Box<dyn Fn() -> Pin<Box<dyn Future<Output = DeltavFlowResult<()>> + Send>> + Send + Sync>;

/// Represents a single extract-load pipeline.
///
/// The `Pipeline` holds a boxed `Source` and `Destination`, a polling `interval`,
/// an optional human readable `name`, and an optional async `cleanup` handler
/// executed when the pipeline is shutting down.
pub struct Pipeline {
    id: String,
    name: Option<String>,
    source: Box<dyn Source>,
    destination: Box<dyn Destination>,
    interval: Duration,
    last_run: Option<Instant>,
    cleanup: Option<CleanupFn>,
    /// When true, inspect payload chunks and emit debug logs with size and first bytes.
    debug_payload: bool,
}  


impl Pipeline {
    /// Create a new `Pipeline` from a `Source` and `Destination`.
    ///
    /// The returned pipeline uses a 3-second default interval which can be
    /// changed with `set_interval` (useful in tests).
    pub fn new(source: Box<dyn Source>, destination: Box<dyn Destination>) -> Self {
        Self { 
            id: Uuid::new_v4().to_string(),
            name: None,
            source, 
            destination,
            interval: Duration::from_secs(3),
            last_run: None,
            cleanup: None,
            debug_payload: false,
        }
    }

    /// Execute one extract-then-load cycle for this pipeline.
    ///
    /// Returns Ok(()) on success; propagated errors from the `Source` or
    /// `Destination` will be returned to the caller.
    pub async fn run(&mut self) -> DeltavFlowResult<()> {
        // Clone a few fields needed inside the payload-inspecting closure so
        // we don't capture `&mut self` for the whole stream lifetime.
        let id_clone = self.id.clone();
        let name_owned = self.name.clone().unwrap_or_else(|| "".to_string());

        let start = Instant::now();
        info!(pipeline_id = %id_clone, pipeline_name = %name_owned, "Starting pipeline run");

        let stream = self.source.extract().await?;
        
        // Extract the stream from DeltavStream (which wraps it in an Arc)
        // We need to clone the Arc and then dereference it to get the DataStream
        let data_stream: deltav_utils::DeltavStream = stream.clone();

        let res = self.destination.load(stream).await;
        self.last_run = Some(Instant::now());
        let elapsed = Instant::now() - start;

        // Metrics: record and increment counters
        let pipeline_label = if name_owned.is_empty() { &id_clone } else { &name_owned };
        metrics::observe_duration(pipeline_label, elapsed.as_secs_f64() * 1000.0);
        metrics::inc_run(pipeline_label);

        match res {
            Ok(()) => info!(pipeline_id = %id_clone, pipeline_name = %name_owned, duration_ms = %elapsed.as_millis(), duration_secs = %elapsed.as_secs_f64(), "Finished pipeline run"),
            Err(ref e) => {
                metrics::inc_failure(pipeline_label);
                error!(pipeline_id = %id_clone, pipeline_name = %name_owned, error = ?e, "Pipeline run failed");
            }
        }

        res

    }

    /// Register an async cleanup handler to run when this pipeline is shutting down.
    ///
    /// The provided closure should return a `Future` which resolves to
    /// `DeltavFlowResult<()>`. Cleanup handlers execute after the worker loop
    /// exits during shutdown.
    pub fn on_shutdown<F, Fut>(&mut self, f: F)
    where
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = DeltavFlowResult<()>> + Send + 'static,
    {
        self.cleanup = Some(Box::new(move || Box::pin(f())));
    }

    /// Set a human readable name for this pipeline (optional).
    ///
    /// Names are included in structured logs when a pipeline runs.
    pub fn set_name<T: Into<String>>(&mut self, name: T) {
        self.name = Some(name.into());
    }

    /// Set the polling interval for this pipeline (useful for tests).
    pub fn set_interval(&mut self, interval: Duration) {
        self.interval = interval;
    }

    /// Enable or disable debug-level payload logging for this pipeline.
    /// When enabled, the worker will log the length and first bytes of each
    /// chunk yielded by the `Source` at `debug` level.
    pub fn enable_debug_payload(&mut self, enabled: bool) {
        self.debug_payload = enabled;
    }

    /// Run the pipeline worker loop until `shutdown_rx` signals shutdown, then
    /// run the optional cleanup handler.
    ///
    /// This is intended for use by the `Scheduler` (or tests) and consumes the
    /// pipeline instance.
    pub async fn worker(mut self, mut shutdown_rx: watch::Receiver<bool>) {
        let mut ticker = tokio::time::interval(self.interval);

        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    if let Err(err) = self.run().await {
                        error!(pipeline_id = %self.id, error = ?err, "Pipeline failed");
                    }
                }
                res = shutdown_rx.changed() => {
                    if res.is_ok() && *shutdown_rx.borrow() {
                        // shutdown requested
                        break;
                    }
                    if res.is_err() {
                        // sender dropped, exit
                        break;
                    }
                }
            }
        }

        // Per-pipeline cleanup
        if let Some(cleanup) = self.cleanup.take() {
            if let Err(err) = (cleanup)().await {
                error!(pipeline_id = %self.id, error = ?err, "Pipeline cleanup error");
            }
        }
    }
}

/// Simple scheduler that runs pipelines concurrently and coordinates
/// graceful shutdown.
///
/// Add pipelines with `add_pipeline` and call `start()` to run until the
/// process receives Ctrl+C. For testability, use `start_with_shutdown` to pass
/// a custom shutdown future (e.g. a oneshot receiver).
pub struct Scheduler {
    pipelines: Vec<Pipeline>,
}

impl Scheduler {
    /// Create an empty scheduler.
    pub fn new() -> Self {
        Self { pipelines: Vec::new() }
    }

    /// Add a pipeline to the scheduler.
    pub fn add_pipeline(&mut self, pipeline: Pipeline) {
        self.pipelines.push(pipeline);
    }

    /// Start the scheduler and use the supplied shutdown future to trigger
    /// graceful shutdown. This is useful for tests and alternative shutdown
    /// strategies.
    pub async fn start_with_shutdown<F>(self, shutdown_signal: F) -> DeltavFlowResult<()>
    where
        F: std::future::Future<Output = ()> + Send + 'static,
    {
        // watch channel used to broadcast shutdown signal to pipeline tasks
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let mut handles = Vec::new();

        info!(num_pipelines = self.pipelines.len(), "Scheduler starting");

        for pipeline in self.pipelines {
            let rx = shutdown_rx.clone();
            let handle = tokio::spawn(async move {
                pipeline.worker(rx).await;
            });

            handles.push(handle);
        }

        // Wait for the provided shutdown future to resolve
        shutdown_signal.await;

        info!("Shutdown signal received, broadcasting to workers");

        // Broadcast shutdown and wait for tasks to finish
        let _ = shutdown_tx.send(true);

        for handle in handles {
            if let Err(err) = handle.await {
                error!(error = ?err, "Pipeline task join error");
            }
        }

        Ok(())
    }

    /// Start the scheduler and use Ctrl+C for shutdown (original behavior).
    pub async fn start(self) -> DeltavFlowResult<()> {
        self.start_with_shutdown(async {
            let _ = signal::ctrl_c().await;
        })
        .await
    }
}