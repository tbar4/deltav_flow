use std::path::Path;
use crate::config::types::PipelineConfig;

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("failed to read config file: {0}")]
    Io(#[from] std::io::Error),
    #[error("failed to parse YAML: {0}")]
    Yaml(#[from] serde_yaml::Error),
}

/// Load a pipeline config from a YAML file.
pub fn load_pipeline(path: impl AsRef<Path>) -> Result<PipelineConfig, ConfigError> {
    let content = std::fs::read_to_string(path)?;
    parse_pipeline(&content)
}

/// Parse a pipeline config from a YAML string.
pub fn parse_pipeline(yaml: &str) -> Result<PipelineConfig, ConfigError> {
    let config: PipelineConfig = serde_yaml::from_str(yaml)?;
    Ok(config)
}

/// Load all pipeline configs from a directory.
pub fn load_pipelines_dir(dir: impl AsRef<Path>) -> Result<Vec<PipelineConfig>, ConfigError> {
    let mut configs = Vec::new();
    let entries = std::fs::read_dir(dir)?;
    for entry in entries {
        let entry = entry?;
        let path = entry.path();
        if path.extension().and_then(|e| e.to_str()) == Some("yaml")
            || path.extension().and_then(|e| e.to_str()) == Some("yml")
        {
            configs.push(load_pipeline(path)?);
        }
    }
    Ok(configs)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_simple_pipeline() {
        let yaml = r#"
pipeline: weather_etl
description: "Fetch weather data"

trigger:
  type: interval
  every: 60s

tasks:
  fetch:
    type: http_source
    method: GET
    url: "https://api.weather.gov/observations"
    produces: raw_observations
    retries: 3
    retry_delay: 5s

  clean:
    type: drop_nulls
    consumes: raw_observations
    produces: clean_observations

  save:
    type: file_destination
    path: "/data/weather.parquet"
    consumes: clean_observations
"#;

        let config = parse_pipeline(yaml).unwrap();
        assert_eq!(config.pipeline, "weather_etl");
        assert_eq!(config.trigger.trigger_type, "interval");
        assert_eq!(config.trigger.every.as_deref(), Some("60s"));

        assert_eq!(config.tasks.len(), 3);

        let fetch = &config.tasks["fetch"];
        assert_eq!(fetch.task_type, "http_source");
        assert_eq!(fetch.produces.as_deref(), Some("raw_observations"));
        assert_eq!(fetch.retries, Some(3));

        let save = &config.tasks["save"];
        assert_eq!(save.task_type, "file_destination");
    }

    #[test]
    fn parse_fan_in_pipeline() {
        let yaml = r#"
pipeline: merge_pipeline
trigger:
  type: webhook
  path: /trigger/merge

tasks:
  fetch_a:
    type: http_source
    url: "https://api.example.com/a"
    produces: data_a

  fetch_b:
    type: http_source
    url: "https://api.example.com/b"
    produces: data_b

  merge:
    type: concat
    consumes:
      - data_a
      - data_b
    produces: merged

  save:
    type: file_destination
    path: "/data/merged.parquet"
    consumes: merged
"#;

        let config = parse_pipeline(yaml).unwrap();
        let merge = &config.tasks["merge"];
        let consumes = merge.consumes.clone().unwrap().into_vec();
        assert_eq!(consumes, vec!["data_a", "data_b"]);
    }
}
