use serde::Deserialize;
use std::collections::HashMap;

#[derive(Debug, Deserialize)]
pub struct PipelineConfig {
    pub pipeline: String,
    pub description: Option<String>,
    pub trigger: TriggerConfig,
    pub tasks: HashMap<String, TaskConfig>,
}

#[derive(Debug, Deserialize)]
pub struct TriggerConfig {
    #[serde(rename = "type")]
    pub trigger_type: String,
    /// For interval triggers: e.g. "60s", "5m"
    pub every: Option<String>,
    /// For cron triggers
    pub expr: Option<String>,
    /// For webhook triggers
    pub path: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct TaskConfig {
    #[serde(rename = "type")]
    pub task_type: String,
    pub produces: Option<String>,
    pub consumes: Option<StringOrVec>,
    pub retries: Option<u32>,
    pub retry_delay: Option<String>,
    /// All other fields are passed to the task implementation as params
    #[serde(flatten)]
    pub params: HashMap<String, serde_yaml::Value>,
}

/// Allows consumes to be either a single string or a list of strings.
#[derive(Debug, Deserialize, Clone)]
#[serde(untagged)]
pub enum StringOrVec {
    Single(String),
    Multiple(Vec<String>),
}

impl StringOrVec {
    pub fn into_vec(self) -> Vec<String> {
        match self {
            StringOrVec::Single(s) => vec![s],
            StringOrVec::Multiple(v) => v,
        }
    }
}
