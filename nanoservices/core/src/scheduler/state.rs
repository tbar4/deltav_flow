use std::collections::{HashMap, HashSet};
use deltav_utils::DeltavStream;

/// Status of a pipeline run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RunStatus {
    Triggered,
    Running,
    Completed,
    Failed,
}

/// Status of an individual task within a run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TaskStatus {
    Waiting,
    Ready,
    Running,
    Completed,
    Failed,
    Skipped,
}

/// In-memory state for a single pipeline run.
#[derive(Debug)]
pub struct RunState {
    pub id: String,
    pub pipeline: String,
    pub status: RunStatus,
    pub task_statuses: HashMap<String, TaskStatus>,
    /// Produced data keyed by data contract name.
    pub data: HashMap<String, DeltavStream>,
    /// Task name -> set of data names it needs.
    pub task_consumes: HashMap<String, HashSet<String>>,
    pub started_at: std::time::Instant,
}

impl RunState {
    pub fn new(
        id: String,
        pipeline: String,
        task_consumes: HashMap<String, HashSet<String>>,
    ) -> Self {
        let mut task_statuses = HashMap::new();
        for (task, deps) in &task_consumes {
            if deps.is_empty() {
                task_statuses.insert(task.clone(), TaskStatus::Ready);
            } else {
                task_statuses.insert(task.clone(), TaskStatus::Waiting);
            }
        }
        Self {
            id,
            pipeline,
            status: RunStatus::Triggered,
            task_statuses,
            data: HashMap::new(),
            task_consumes,
            started_at: std::time::Instant::now(),
        }
    }

    /// Record that a task completed and produced data. Returns newly ready tasks.
    pub fn task_completed(
        &mut self,
        task_name: &str,
        produced_data_name: Option<&str>,
        data: Option<DeltavStream>,
    ) -> Vec<String> {
        self.task_statuses.insert(task_name.to_string(), TaskStatus::Completed);

        if let (Some(name), Some(stream)) = (produced_data_name, data) {
            self.data.insert(name.to_string(), stream);
        }

        // Check which waiting tasks are now ready
        let mut newly_ready = Vec::new();
        for (name, status) in &self.task_statuses {
            if *status != TaskStatus::Waiting {
                continue;
            }
            if let Some(deps) = self.task_consumes.get(name) {
                if deps.iter().all(|d| self.data.contains_key(d)) {
                    newly_ready.push(name.clone());
                }
            }
        }

        for name in &newly_ready {
            self.task_statuses.insert(name.clone(), TaskStatus::Ready);
        }

        // Check if all tasks are done
        let all_done = self.task_statuses.values().all(|s| {
            matches!(s, TaskStatus::Completed | TaskStatus::Failed | TaskStatus::Skipped)
        });
        if all_done {
            let any_failed = self.task_statuses.values().any(|s| *s == TaskStatus::Failed);
            self.status = if any_failed { RunStatus::Failed } else { RunStatus::Completed };
        } else {
            self.status = RunStatus::Running;
        }

        newly_ready
    }

    /// Mark a task as failed. Skips all downstream tasks. Returns true if run is now complete.
    pub fn task_failed(&mut self, task_name: &str) -> bool {
        self.task_statuses.insert(task_name.to_string(), TaskStatus::Failed);

        let all_done = self.task_statuses.values().all(|s| {
            !matches!(s, TaskStatus::Ready | TaskStatus::Running)
        });

        if all_done {
            // Skip any still-waiting tasks
            let waiting: Vec<String> = self.task_statuses.iter()
                .filter(|(_, s)| **s == TaskStatus::Waiting)
                .map(|(n, _)| n.clone())
                .collect();
            for name in waiting {
                self.task_statuses.insert(name, TaskStatus::Skipped);
            }
            self.status = RunStatus::Failed;
            true
        } else {
            false
        }
    }

    /// Get all tasks in Ready status.
    pub fn ready_tasks(&self) -> Vec<String> {
        self.task_statuses.iter()
            .filter(|(_, s)| **s == TaskStatus::Ready)
            .map(|(n, _)| n.clone())
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use std::sync::Arc;

    fn test_stream() -> DeltavStream {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        ).unwrap();
        DeltavStream::new(vec![batch])
    }

    #[test]
    fn root_tasks_start_ready() {
        let mut consumes = HashMap::new();
        consumes.insert("fetch".to_string(), HashSet::new());
        consumes.insert("clean".to_string(), ["raw".to_string()].into());
        consumes.insert("save".to_string(), ["clean_data".to_string()].into());

        let state = RunState::new("run-1".to_string(), "test".to_string(), consumes);

        assert_eq!(state.task_statuses["fetch"], TaskStatus::Ready);
        assert_eq!(state.task_statuses["clean"], TaskStatus::Waiting);
        assert_eq!(state.task_statuses["save"], TaskStatus::Waiting);
    }

    #[test]
    fn completing_task_makes_dependents_ready() {
        let mut consumes = HashMap::new();
        consumes.insert("fetch".to_string(), HashSet::new());
        consumes.insert("clean".to_string(), ["raw".to_string()].into());
        consumes.insert("save".to_string(), ["clean_data".to_string()].into());

        let mut state = RunState::new("run-1".to_string(), "test".to_string(), consumes);

        let newly_ready = state.task_completed("fetch", Some("raw"), Some(test_stream()));
        assert_eq!(newly_ready, vec!["clean"]);
        assert_eq!(state.task_statuses["clean"], TaskStatus::Ready);
        assert_eq!(state.task_statuses["save"], TaskStatus::Waiting);
    }

    #[test]
    fn run_completes_when_all_tasks_done() {
        let mut consumes = HashMap::new();
        consumes.insert("fetch".to_string(), HashSet::new());
        consumes.insert("save".to_string(), ["raw".to_string()].into());

        let mut state = RunState::new("run-1".to_string(), "test".to_string(), consumes);

        state.task_completed("fetch", Some("raw"), Some(test_stream()));
        state.task_statuses.insert("save".to_string(), TaskStatus::Running);
        state.task_completed("save", None, None);

        assert_eq!(state.status, RunStatus::Completed);
    }

    #[test]
    fn task_failure_marks_run_failed() {
        let mut consumes = HashMap::new();
        consumes.insert("fetch".to_string(), HashSet::new());
        consumes.insert("save".to_string(), ["raw".to_string()].into());

        let mut state = RunState::new("run-1".to_string(), "test".to_string(), consumes);

        let complete = state.task_failed("fetch");
        assert!(complete); // no running tasks, save is waiting -> skip -> done
        assert_eq!(state.status, RunStatus::Failed);
        assert_eq!(state.task_statuses["save"], TaskStatus::Skipped);
    }
}
