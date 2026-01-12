# deltav-flow usage & examples

This document contains quick examples and guidance for running pipelines and the metrics exporter.

## Start a metrics exporter (graceful shutdown)

```no_run
use std::net::TcpListener;
use tokio::signal;
use deltav_core::metrics;

let listener = TcpListener::bind("127.0.0.1:0").unwrap();
let addr = listener.local_addr().unwrap();
println!("metrics available at http://{}:{}/metrics", addr.ip(), addr.port());

let shutdown = async { let _ = signal::ctrl_c().await; };
let _handle = metrics::start_exporter_with_shutdown(listener, shutdown);
```

## Configure pipelines & scheduler

- Create sources (HTTP, file, custom) and destinations.
- Build `Pipeline` instances, set names, intervals, and optional debug payload logging.
- Add cleanup hooks with `Pipeline::on_shutdown()` for per-pipeline shutdown work.
- Use `Scheduler::start_with_shutdown()` to run until a provided shutdown future resolves.

Example (async context):

```no_run
use deltav_flow::core::{pipeline::Scheduler, pipeline::Pipeline, sources::http_client::HttpSource, destinations::file::FileDestination};
use deltav_flow::core::sources::Source;

let source: Box<dyn Source> = Box::new(HttpSource::default());
let dest = Box::new(FileDestination("/tmp/data.blob".to_string()));
let mut p = Pipeline::new(source, dest);
p.set_name("example");
p.set_interval(std::time::Duration::from_secs(60));

let mut sched = Scheduler::new();
sched.add_pipeline(p);

// Shutdown on Ctrl-C
let shutdown = async { let _ = tokio::signal::ctrl_c().await; };
sched.start_with_shutdown(shutdown).await.unwrap();
```

## Testing tips

- Use `start_exporter` with a `TcpListener` bound to `127.0.0.1:0` to get an ephemeral port for tests.
- Use `metrics::gather_text()` to inspect metrics without needing a running HTTP server.

---
