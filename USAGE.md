# deltav-flow Usage Guide

## Overview

deltav-flow is a modular Rust pipeline scheduler and nanoservices framework. It provides:
- A robust async scheduler for running pipelines at intervals
- Pluggable sources and destinations
- Structured logging and Prometheus metrics exporter
- Graceful shutdown and per-pipeline cleanup

## Example: Pipeline and Scheduler

```rust
use deltav_core::{Pipeline, Scheduler, logging, metrics};
use deltav_core::sources::DummySource;
use deltav_core::destinations::DummyDestination;
use std::time::Duration;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize logging and start metrics exporter
    logging::init();
    let (metrics_handle, shutdown_tx) = metrics::start_exporter(([127,0,0,1], 9000).into());

    // Create a pipeline
    let pipeline = Pipeline::new(
        Box::new(DummySource::default()),
        Box::new(DummyDestination::default()),
        Duration::from_secs(10),
    ).with_name("example-pipeline");

    // Create and run the scheduler
    let mut scheduler = Scheduler::new();
    scheduler.add_pipeline(pipeline);
    scheduler.start().await?;

    // On shutdown, stop metrics exporter
    let _ = shutdown_tx.send(());
    let _ = metrics_handle.await;
    Ok(())
}
```

## Metrics Exporter

- Exposes Prometheus metrics at `/metrics` on the configured port.
- Use `metrics::gather_text()` for direct text output in tests.
- Graceful shutdown supported via oneshot sender.

## Features
- Async pipeline execution and interval scheduling
- Per-pipeline cleanup hooks
- Structured logging (tracing)
- Prometheus metrics exporter (HTTP)
- Unit and integration tests for scheduler, metrics, and shutdown

## See Also
- [nanoservices/core/README.md](nanoservices/core/README.md) for crate-level details
- [examples/flow.rs](examples/flow.rs) for a real-world example
