//! deltav_core — pipeline scheduler and integration primitives
//!
//! This crate provides the core building blocks for creating simple extract-then-load
//! pipelines and a scheduler that runs them on intervals with graceful shutdown and
//! per-pipeline cleanup hooks.
//!
//! Basic usage:
//!
//! ```no_run
//! use deltav_core::pipeline::{Scheduler, Pipeline};
//! use deltav_core::sources::http_client::HttpSource;
//! use deltav_core::destinations::file::FileDestination;
//!
//! let source: Box<dyn deltav_core::sources::Source> = Box::new(HttpSource::default());
//! let dest: Box<dyn deltav_core::destinations::Destination> = Box::new(FileDestination("out.blob".into()));
//! let mut pipeline = Pipeline::new(source, dest);
//! pipeline.set_name("example");
//! let mut scheduler = Scheduler::new();
//! scheduler.add_pipeline(pipeline);
//! // call `scheduler.start().await` from a tokio runtime to run continuously
//! ```

pub mod sources;
pub mod destinations;
pub mod transforms;
pub mod dag;
pub mod events;
pub mod store;
pub mod scheduler;
pub mod config;
pub mod builder;
pub mod pipeline;

pub mod logging;

pub mod metrics;