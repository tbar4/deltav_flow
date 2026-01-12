# deltav_core — Metrics exporter

This crate provides the core pipeline scheduler and a lightweight Prometheus metrics exporter.

## Metrics exporter

- Use `deltav_core::metrics::gather_text()` to get the Prometheus text exposition format for testing or custom exporters.
- Use `deltav_core::metrics::start_exporter_with_shutdown(listener, shutdown_future)` to start an HTTP exporter that serves `/metrics`. Provide a bound `TcpListener` so you can bind to port `0` (ephemeral port) in tests and discover the actual port.
- Convenience wrapper `deltav_core::metrics::start_exporter(listener)` returns a `(JoinHandle, oneshot::Sender)` pair that you can use to request a graceful shutdown.

### Example

```no_run
use std::net::TcpListener;
use tokio::signal;
use deltav_core::metrics;

// Bind to an ephemeral port
let listener = TcpListener::bind("127.0.0.1:0").unwrap();
let addr = listener.local_addr().unwrap();
println!("metrics available at http://{}:{}/metrics", addr.ip(), addr.port());

// Shutdown when Ctrl-C is pressed
let shutdown = async { let _ = signal::ctrl_c().await; };
let _handle = metrics::start_exporter_with_shutdown(listener, shutdown);
```

## Notes

- The exporter runs on a dedicated Tokio runtime to make it easy to start from synchronous tests or non-async contexts.
- Tests in this crate exercise the exporter; see `nanoservices/core/tests/exporter_integration.rs` for an example of starting the exporter on an ephemeral port and requesting shutdown.

If you want a more advanced HTTP server (routing, middleware), we can swap the implementation to a higher-level framework like Axum in a follow-up change.

---

## Scheduler & Pipeline — quick usage guide

This project provides a lightweight scheduler and pipeline abstraction for periodically fetching data from a `Source` and sending it to a `Destination`.

### Basic pipeline

```no_run
use deltav_flow::core::{pipeline::Pipeline, sources::HttpSource, destinations::file::FileDestination};
use deltav_flow::core::sources::Source;
use std::time::Duration;

let source: Box<dyn Source> = Box::new(HttpSource::default());
let destination = Box::new(FileDestination("/tmp/out.blob".to_string()));
let mut pipeline = Pipeline::new(source, destination);

pipeline.set_name("http_default");
pipeline.set_interval(std::time::Duration::from_secs(30));
pipeline.enable_debug_payload(false); // enable to log payloads at debug level

// Add an async cleanup hook to run when the scheduler shuts down
pipeline.on_shutdown(|| async {
    // cleanup work here
});

let mut scheduler = deltav_flow::core::pipeline::Scheduler::new();
scheduler.add_pipeline(pipeline);

// Start scheduler and block until Ctrl-C (or another shutdown signal) — the
// scheduler supports graceful shutdown and will run per-pipeline cleanup hooks.
let shutdown = async { let _ = tokio::signal::ctrl_c().await; };
scheduler.start_with_shutdown(shutdown).await?;
```

### Notes & tips

- Use `Pipeline::set_interval` to control the periodic run interval for each pipeline.
- Use `Pipeline::enable_debug_payload(true)` to emit debug logs of payload bytes (useful during development).
- The `Scheduler::start_with_shutdown(shutdown_future)` API lets you provide a shutdown future (e.g. `tokio::signal::ctrl_c()`) for deterministic shutdown in tests or production.
- Metrics are available via the built-in Prometheus exporter (see the metrics section above).

---
