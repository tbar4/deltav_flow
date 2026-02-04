use std::time::Duration;

/// The type of event that triggers a pipeline run.
#[derive(Debug, Clone)]
pub enum Trigger {
    /// Run on a fixed interval
    Interval(Duration),
    /// Run when an HTTP webhook is received
    Webhook { path: String },
    /// Run when a file appears or changes at the given path
    FileWatch { path: String },
}

/// An event delivered to the scheduler.
#[derive(Debug, Clone)]
pub struct TriggerEvent {
    /// Which pipeline this event is for
    pub pipeline: String,
    /// What kind of trigger caused this event
    pub trigger: Trigger,
}
