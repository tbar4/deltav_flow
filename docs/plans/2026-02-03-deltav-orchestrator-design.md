# DeltaV: Event-Driven Data Pipeline Orchestrator

**Date:** 2026-02-03
**Status:** Draft

## Overview

DeltaV is a single-binary data pipeline orchestrator built in Rust. It addresses the core pain points of Apache Airflow — scheduler bottleneck, painful local dev experience, brittle DAG wiring, and fragmented observability — by inverting Airflow's architecture: event-driven scheduling, data-dependency DAGs, in-memory hot state, and built-in observability.

**Target user:** Individual developers and small teams who want a local-first orchestrator that just works, with a single binary and zero infrastructure dependencies.

**Binary name:** `deltav`

## Architecture

```
┌──────────────────────────────────────────────────────────┐
│                     Single Binary                         │
│                                                           │
│  ┌─────────────┐    ┌──────────────┐                     │
│  │ Config Loader│───>│  DAG Builder  │                    │
│  │ (YAML/TOML) │    │ (infers deps  │                    │
│  └─────────────┘    │  from data    │                    │
│                     │  contracts)   │                     │
│                     └──────┬───────┘                      │
│                            v                              │
│  ┌──────────┐     ┌───────────────┐   ┌───────────────┐ │
│  │  Event    │────>│   Scheduler   │──>│  Task         │ │
│  │  Bus      │<───│ (in-memory    │   │  Executor     │ │
│  │(triggers) │    │  hot state)   │   │  (tokio)      │ │
│  └──────────┘     └───────┬───────┘   └───────────────┘ │
│                           │                              │
│                    ┌──────v───────┐                       │
│                    │   SQLite     │                       │
│                    │ (run history,│                       │
│                    │  audit log)  │                       │
│                    └──────────────┘                       │
└──────────────────────────────────────────────────────────┘
```

Five core components:

- **Config Loader** parses YAML pipeline definitions and discovers Rust plugin traits
- **DAG Builder** infers execution order from data contracts — tasks declare inputs/outputs, not explicit ordering
- **Event Bus** receives triggers (timers, webhooks, file watches) and routes them to the scheduler
- **Scheduler** maintains in-memory state (queued, running, waiting-on-data) and dispatches ready tasks to the executor
- **Task Executor** runs tasks concurrently on the tokio runtime, passing `DeltavStream` between tasks via the data contract

## Data Contract & DAG Resolution

Tasks don't declare dependencies on other tasks — they declare dependencies on data. Each task has a data contract specifying what it `produces` and/or `consumes`.

Example:

```yaml
pipeline: weather_etl
trigger:
  interval: 60s

tasks:
  fetch_weather:
    source: http
    url: "https://api.weather.gov/stations/KNYC/observations"
    produces: raw_weather

  clean_nulls:
    transform: drop_nulls
    consumes: raw_weather
    produces: clean_weather

  to_parquet:
    destination: file
    path: "/data/weather.parquet"
    consumes: clean_weather
```

The DAG Builder reads these contracts and infers the graph:

```
fetch_weather --(raw_weather)--> clean_nulls --(clean_weather)--> to_parquet
```

### Resolution Rules

- Every `consumes` must match exactly one `produces` within the pipeline — ambiguity is a load-time error
- Tasks with no `consumes` are root nodes (sources)
- Tasks with no `produces` are leaf nodes (destinations)
- Cycles are detected at load time and rejected
- Fan-out is natural: two tasks can consume the same named output
- Fan-in requires a merge transform that consumes multiple named inputs

The `DeltavStream` (`Arc<Vec<RecordBatch>>`) is the runtime representation of every named data contract.

## Event-Driven Scheduler

The scheduler reacts to events rather than polling. This is the core architectural inversion from Airflow.

### Trigger Types

```
┌─────────────┐  ┌─────────────┐  ┌─────────────┐
│ Timer Event  │  │ Webhook     │  │ File Watch  │
│ (interval/   │  │ (HTTP POST  │  │ (fs notify  │
│  cron)       │  │  to /trigger)│  │  on path)   │
└──────┬──────┘  └──────┬──────┘  └──────┬──────┘
       │                │                │
       v                v                v
   ┌──────────────────────────────────────┐
   │          Event Bus (mpsc channel)     │
   └──────────────────┬───────────────────┘
                      v
              ┌───────────────┐
              │   Scheduler   │
              └───────────────┘
```

### Pipeline Run State Machine

```
Triggered --> Resolving --> Running --> Completed
                             |
                             ├──> Failed (retryable)
                             └──> Dead (max retries exceeded)
```

### Scheduler Loop

When the scheduler receives a trigger event:

1. Create a pipeline run in `Triggered` state
2. Move to `Resolving` — identify root tasks (no `consumes`) and mark them `Ready`
3. Dispatch `Ready` tasks to the tokio executor
4. When a task completes, store its output `DeltavStream` in a run-scoped hashmap keyed by the `produces` name
5. Check all downstream tasks — if all their `consumes` keys are now present in the hashmap, mark them `Ready` and dispatch
6. When all leaf tasks complete, mark the run `Completed` and flush to SQLite

The scheduler loop is a single `tokio::select!` over the event bus channel and a task completion channel. No polling. No DB queries in the hot path. State transitions are hashmap lookups and channel sends.

### Retry Logic

Per-task, configured in YAML:

```yaml
tasks:
  fetch_weather:
    source: http
    retries: 3
    retry_delay: 5s
```

Failed tasks re-enter the `Ready` queue after the delay. If max retries are exceeded, the task goes `Dead` and downstream tasks are marked `Skipped`.

## SQLite Persistence & Crash Recovery

### Write Points

| Event | When Written | Data |
|-------|-------------|------|
| Pipeline run started | On `Triggered` | run_id, pipeline_name, trigger_type, timestamp |
| Task completed | On `Completed` or `Failed` | run_id, task_name, status, duration_ms, error |
| Pipeline run finished | On all tasks done | run_id, final_status, total_duration_ms |

Three writes per run, not per state transition. Airflow hits Postgres on every state change.

### Schema

```sql
CREATE TABLE pipeline_runs (
    id TEXT PRIMARY KEY,
    pipeline TEXT NOT NULL,
    trigger_type TEXT NOT NULL,
    status TEXT NOT NULL,
    started_at TEXT NOT NULL,
    finished_at TEXT,
    duration_ms INTEGER
);

CREATE TABLE task_runs (
    id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES pipeline_runs(id),
    task_name TEXT NOT NULL,
    status TEXT NOT NULL,
    started_at TEXT NOT NULL,
    finished_at TEXT,
    duration_ms INTEGER,
    error TEXT
);
```

No ORM. Raw SQL via `rusqlite`.

### Crash Recovery

- On startup, query SQLite for runs in `Triggered` or `Running` state
- Mark those runs as `Crashed`
- Do not attempt to resume — re-trigger on next event
- Safe because tasks should be idempotent

## Rust Plugin Traits

Three core traits define the extensibility surface:

```rust
#[async_trait]
pub trait Source: Send + Sync {
    /// Human-readable name for logging and metrics
    fn name(&self) -> &str;

    /// Declare what this source produces
    fn produces(&self) -> &str;

    /// Extract data. Called once per pipeline trigger.
    async fn extract(&self) -> DeltavFlowResult<DeltavStream>;
}

#[async_trait]
pub trait Transform: Send + Sync {
    fn name(&self) -> &str;

    /// Declare inputs and output
    fn consumes(&self) -> Vec<&str>;
    fn produces(&self) -> &str;

    /// Transform one or more input streams into one output stream
    async fn transform(
        &self,
        inputs: HashMap<String, DeltavStream>,
    ) -> DeltavFlowResult<DeltavStream>;
}

#[async_trait]
pub trait Destination: Send + Sync {
    fn name(&self) -> &str;

    fn consumes(&self) -> &str;

    /// Load data to final destination. Called once per pipeline trigger.
    async fn load(&self, stream: DeltavStream) -> DeltavFlowResult<()>;
}
```

### Design Notes

- `name()` enables observability — every log line and metric identifies the task
- `produces()` / `consumes()` make the data contract inspectable at runtime for DAG building
- `Transform` takes `HashMap<String, DeltavStream>` to support fan-in
- `Destination::load` takes `&self` not `&mut self` — destinations should be stateless between runs
- Traits are designed so a WASM adapter layer can wrap them later without changing the core

### Built-in Implementations

- **Sources:** `HttpSource`, `FileSource`
- **Transforms:** `DropNulls`, `SelectColumns`, `FilterRows` (Polars-backed)
- **Destinations:** `FileDestination`, `StdoutDestination`

## Hybrid Pipeline Definition

### YAML Config (common 80%)

```yaml
pipeline: weather_etl
description: "Fetch weather data and store as parquet"

trigger:
  type: interval
  every: 60s
  # type: cron
  # expr: "0 */6 * * *"
  # type: webhook
  # path: /trigger/weather
  # type: file_watch
  # path: /data/incoming/

tasks:
  fetch:
    type: http_source
    method: GET
    url: "https://api.weather.gov/stations/KNYC/observations"
    headers:
      Accept: "application/json"
    produces: raw_observations
    retries: 3
    retry_delay: 5s

  clean:
    type: drop_nulls
    consumes: raw_observations
    produces: clean_observations

  save:
    type: file_destination
    path: "/data/weather_{{run_date}}.parquet"
    format: parquet
    consumes: clean_observations
```

### Rust Code (custom logic)

```rust
let pipeline = PipelineBuilder::new("weather_etl")
    .trigger(Trigger::interval(Duration::from_secs(60)))
    .source(MyCustomSource::new())
    .transform(DropNulls::new())
    .destination(FileDestination::new("/data/out.parquet"))
    .build()?;

DeltavFlow::new()
    .add_pipeline(pipeline)
    .run()
    .await?;
```

### Task Type Registry

Config loader maps type name strings to factory functions:

```
"http_source"       -> builds HttpSource
"file_source"       -> builds FileSource
"drop_nulls"        -> builds DropNulls
"file_destination"  -> builds FileDestination
"custom:my_plugin"  -> looks up user-registered plugin
```

### Hot Reload

DeltaV watches a `pipelines/` directory. Drop a YAML file in, it picks it up on next trigger cycle. Delete it, the pipeline stops. No restart required for config-only pipelines.

## Observability

### Structured Logging

Every log line includes pipeline name, task name, and run ID as structured fields via `tracing`:

```
2026-02-03T10:00:01Z INFO  run=a3f2 pipeline=weather_etl task=fetch status=started
2026-02-03T10:00:02Z INFO  run=a3f2 pipeline=weather_etl task=fetch status=completed duration_ms=1200 rows=150
2026-02-03T10:00:02Z ERROR run=a3f2 pipeline=weather_etl task=clean status=failed error="schema mismatch"
```

### Prometheus Metrics

Exposed on `/metrics` endpoint:

- `deltav_pipeline_runs_total{pipeline, trigger_type, status}` — counter
- `deltav_task_duration_ms{pipeline, task, status}` — histogram
- `deltav_task_runs_total{pipeline, task, status}` — counter
- `deltav_scheduler_queue_depth` — gauge
- `deltav_event_bus_events_total{trigger_type}` — counter

### CLI Query Interface

```bash
deltav history                      # last 20 runs
deltav history weather_etl          # runs for one pipeline
deltav history --status failed      # failed runs
deltav inspect run a3f2             # task breakdown for a run
deltav status                       # currently running pipelines
```

Single binary is both orchestrator and inspection tool. Web UI is a future concern — SQLite and metrics endpoint provide the data layer.

## Key Design Decisions Summary

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Target user | Individual dev, local-first | Start simple, grow into teams |
| Pipeline definition | Hybrid YAML + Rust | Config for 80%, code for 20% |
| Scheduler model | Event-driven, in-memory | Eliminates Airflow's DB polling bottleneck |
| DAG model | Data-dependency | Self-documenting, self-correcting graphs |
| Extensibility | Rust traits | Type-safe, designed for future WASM |
| Persistence | SQLite (history) + in-memory (hot) | Fast scheduler, durable audit trail |
| Observability | Logs + metrics + CLI | Built-in, zero setup |
| Triggers | Event-driven (interval, webhook, file) | Interval as special case, not the default |
| Crash recovery | Mark crashed, re-trigger | Simple, relies on task idempotency |
