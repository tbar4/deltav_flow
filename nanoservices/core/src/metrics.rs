use prometheus::{Encoder, TextEncoder, Registry, HistogramVec, CounterVec, Opts};
use once_cell::sync::Lazy;
use std::net::TcpListener;
use tokio::task::JoinHandle;
use std::convert::Infallible;
use hyper::{Body, Request, Response, Method, StatusCode};
use hyper::service::service_fn;

// Global registry and metrics are initialized lazily.
static REGISTRY: Lazy<Registry> = Lazy::new(|| Registry::new_custom(Some("deltav_core".to_string()), None).unwrap());

static PIPELINE_DURATION_MS: Lazy<HistogramVec> = Lazy::new(|| {
    let opts = Opts::new("pipeline_duration_ms", "Pipeline run duration in milliseconds");
    let hist = HistogramVec::new(prometheus::HistogramOpts::from(opts), &["pipeline_name"]).unwrap();
    REGISTRY.register(Box::new(hist.clone())).ok();
    hist
});

static PIPELINE_RUNS: Lazy<CounterVec> = Lazy::new(|| {
    let opts = Opts::new("pipeline_runs_total", "Total pipeline runs");
    let c = CounterVec::new(opts, &["pipeline_name"]).unwrap();
    REGISTRY.register(Box::new(c.clone())).ok();
    c
});

static PIPELINE_FAILURES: Lazy<CounterVec> = Lazy::new(|| {
    let opts = Opts::new("pipeline_failures_total", "Total failed pipeline runs");
    let c = CounterVec::new(opts, &["pipeline_name"]).unwrap();
    REGISTRY.register(Box::new(c.clone())).ok();
    c
});

/// Observe a pipeline run duration in milliseconds.
pub fn observe_duration(pipeline_name: &str, duration_ms: f64) {
    PIPELINE_DURATION_MS.with_label_values(&[pipeline_name]).observe(duration_ms);
}

/// Increment the run counter for a pipeline.
pub fn inc_run(pipeline_name: &str) {
    PIPELINE_RUNS.with_label_values(&[pipeline_name]).inc();
}

/// Increment the failure counter for a pipeline.
pub fn inc_failure(pipeline_name: &str) {
    PIPELINE_FAILURES.with_label_values(&[pipeline_name]).inc();
}

/// Gather metrics as text in Prometheus exposition format.
///
/// This returns a string containing the text-format exposition of the current
/// registry. It is useful for tests or when implementing a custom exporter.
///
/// # Example
///
/// ```no_run
/// use deltav_core::metrics;
/// // record a metric
/// metrics::inc_run("example_pipeline");
/// // gather text for exposition
/// let body = metrics::gather_text();
/// println!("metrics:\n{}", body);
/// ```
pub fn gather_text() -> String {
    let metric_families = REGISTRY.gather();
    let mut buffer = vec![];
    let encoder = TextEncoder::new();
    encoder.encode(&metric_families, &mut buffer).unwrap();
    String::from_utf8(buffer).unwrap()
}

/// Start an HTTP exporter task that serves `/metrics` and supports graceful shutdown.
///
/// The exporter listens on the provided `TcpListener` and serves `/metrics` in the
/// Prometheus text exposition format. The server runs on a dedicated Tokio runtime
/// and will stop accepting new connections when the provided `shutdown` future
/// resolves.
///
/// # Notes
/// - This function returns a `JoinHandle<()>` for the background task. In tests
///   or applications that start the exporter from synchronous code you can abort
///   the handle to stop the task. Prefer providing a `shutdown` future where
///   possible for graceful termination.
///
/// # Example (uses Ctrl-C for shutdown)
///
/// ```no_run
/// use std::net::TcpListener;
/// use deltav_core::metrics;
///
/// // Bind to an ephemeral port and start the exporter.
/// let listener = TcpListener::bind("127.0.0.1:0").unwrap();
/// let addr = listener.local_addr().unwrap();
/// println!("metrics running at http://{}:{}/metrics", addr.ip(), addr.port());
///
/// // Shutdown the exporter when the user presses Ctrl-C
/// let shutdown = async { let _ = tokio::signal::ctrl_c().await; };
/// let _handle = metrics::start_exporter_with_shutdown(listener, shutdown);
/// ```
pub fn start_exporter_with_shutdown<F>(listener: TcpListener, shutdown: F) -> JoinHandle<()> 
where
    F: std::future::Future<Output = ()> + Send + 'static,
{
    // Create a dedicated Tokio runtime for the exporter when one is not
    // already present. We leak the runtime to keep it alive for the lifetime
    // of the exporter; the returned `JoinHandle` can be awaited or aborted by
    // the caller to stop the server task.
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("failed to create tokio runtime for exporter");

    let join = rt.handle().spawn(async move {
        // Ensure the std listener is non-blocking before converting to Tokio.
        if let Err(e) = listener.set_nonblocking(true) {
            tracing::error!(error = ?e, "failed to set listener to non-blocking");
            return;
        }

        // Convert the std TcpListener into a Tokio listener and accept connections
        // ourselves, serving each connection with hyper's HTTP connection API.
        let mut shutdown_fut = Box::pin(shutdown);
        let tcp_listener = tokio::net::TcpListener::from_std(listener)
            .expect("failed to convert listener to tokio listener");

        loop {
            tokio::select! {
                _ = &mut shutdown_fut => {
                    tracing::info!("metrics exporter shutdown requested");
                    break;
                }
                accept = tcp_listener.accept() => {
                    match accept {
                        Ok((stream, _peer)) => {
                            let svc = service_fn(|req: Request<Body>| async move {
                                match (req.method(), req.uri().path()) {
                                    (&Method::GET, "/metrics") => {
                                        let body = gather_text();
                                        Ok::<_, Infallible>(Response::builder()
                                            .status(StatusCode::OK)
                                            .header("content-type", "text/plain; version=0.0.4")
                                            .body(Body::from(body))
                                            .unwrap())
                                    }
                                    _ => Ok(Response::builder().status(StatusCode::NOT_FOUND).body(Body::empty()).unwrap()),
                                }
                            });

                            tokio::spawn(async move {
                                if let Err(err) = hyper::server::conn::Http::new().serve_connection(stream, svc).await {
                                    tracing::error!(error = ?err, "connection serve error");
                                }
                            });
                        }
                        Err(e) => {
                            tracing::error!(error = ?e, "failed to accept connection on metrics listener");
                            break;
                        }
                    }
                }
            }
        }

        tracing::info!("metrics exporter stopped");
    });

    // Keep the runtime alive by leaking it (tests are short-lived; leaking is OK
    // for now).
    Box::leak(Box::new(rt));
    join
}

/// Convenience wrapper kept for backward compatibility that returns the JoinHandle
/// and a oneshot sender the caller can use to request shutdown.
///
/// # Example
///
/// ```no_run
/// use std::net::TcpListener;
/// use std::thread;
/// use std::time::Duration;
/// use deltav_core::metrics;
///
/// let listener = TcpListener::bind("127.0.0.1:0").unwrap();
/// let (handle, shutdown_tx) = metrics::start_exporter(listener);
/// // ... interact with exporter, then request shutdown:
/// shutdown_tx.send(()).unwrap();
/// // give the exporter a moment to stop
/// thread::sleep(Duration::from_millis(50));
/// // Optionally abort/wait on handle if desired
/// handle.abort();
/// ```
pub fn start_exporter(listener: TcpListener) -> (JoinHandle<()>, tokio::sync::oneshot::Sender<()>) {
    let (tx, rx) = tokio::sync::oneshot::channel();
    let handle = start_exporter_with_shutdown(listener, async move { let _ = rx.await; });
    (handle, tx)
}

#[cfg(test)]
pub fn reset_for_tests() {
    // Note: prometheus crate does not provide an easy way to clear metrics; tests
    // can rely on the label-values used in calls to query after runs. We provide
    // a helper for compile-time visibility only.
}