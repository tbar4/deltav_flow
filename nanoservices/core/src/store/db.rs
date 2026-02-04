use rusqlite::{Connection, params};
use std::path::Path;

/// SQLite-backed store for pipeline run history.
pub struct Store {
    conn: Connection,
}

impl Store {
    /// Open or create a SQLite database at the given path.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, rusqlite::Error> {
        let conn = Connection::open(path)?;
        let store = Self { conn };
        store.migrate()?;
        Ok(store)
    }

    /// Create an in-memory database (for testing).
    pub fn in_memory() -> Result<Self, rusqlite::Error> {
        let conn = Connection::open_in_memory()?;
        let store = Self { conn };
        store.migrate()?;
        Ok(store)
    }

    fn migrate(&self) -> Result<(), rusqlite::Error> {
        self.conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS pipeline_runs (
                id TEXT PRIMARY KEY,
                pipeline TEXT NOT NULL,
                trigger_type TEXT NOT NULL,
                status TEXT NOT NULL,
                started_at TEXT NOT NULL,
                finished_at TEXT,
                duration_ms INTEGER
            );
            CREATE TABLE IF NOT EXISTS task_runs (
                id TEXT PRIMARY KEY,
                run_id TEXT NOT NULL REFERENCES pipeline_runs(id),
                task_name TEXT NOT NULL,
                status TEXT NOT NULL,
                started_at TEXT NOT NULL,
                finished_at TEXT,
                duration_ms INTEGER,
                error TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_pipeline_runs_pipeline ON pipeline_runs(pipeline);
            CREATE INDEX IF NOT EXISTS idx_pipeline_runs_status ON pipeline_runs(status);
            CREATE INDEX IF NOT EXISTS idx_task_runs_run_id ON task_runs(run_id);"
        )?;
        Ok(())
    }

    /// Record a new pipeline run as started.
    pub fn insert_pipeline_run(
        &self,
        id: &str,
        pipeline: &str,
        trigger_type: &str,
        started_at: &str,
    ) -> Result<(), rusqlite::Error> {
        self.conn.execute(
            "INSERT INTO pipeline_runs (id, pipeline, trigger_type, status, started_at)
             VALUES (?1, ?2, ?3, 'running', ?4)",
            params![id, pipeline, trigger_type, started_at],
        )?;
        Ok(())
    }

    /// Complete a pipeline run.
    pub fn complete_pipeline_run(
        &self,
        id: &str,
        status: &str,
        finished_at: &str,
        duration_ms: i64,
    ) -> Result<(), rusqlite::Error> {
        self.conn.execute(
            "UPDATE pipeline_runs SET status = ?2, finished_at = ?3, duration_ms = ?4 WHERE id = ?1",
            params![id, status, finished_at, duration_ms],
        )?;
        Ok(())
    }

    /// Record a task run.
    pub fn insert_task_run(
        &self,
        id: &str,
        run_id: &str,
        task_name: &str,
        status: &str,
        started_at: &str,
        finished_at: Option<&str>,
        duration_ms: Option<i64>,
        error: Option<&str>,
    ) -> Result<(), rusqlite::Error> {
        self.conn.execute(
            "INSERT INTO task_runs (id, run_id, task_name, status, started_at, finished_at, duration_ms, error)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            params![id, run_id, task_name, status, started_at, finished_at, duration_ms, error],
        )?;
        Ok(())
    }

    /// Mark in-flight runs as crashed (for crash recovery on startup).
    pub fn mark_inflight_as_crashed(&self) -> Result<usize, rusqlite::Error> {
        let count = self.conn.execute(
            "UPDATE pipeline_runs SET status = 'crashed' WHERE status = 'running'",
            [],
        )?;
        Ok(count)
    }

    /// Get recent pipeline runs.
    pub fn recent_runs(&self, limit: usize) -> Result<Vec<PipelineRunRow>, rusqlite::Error> {
        let mut stmt = self.conn.prepare(
            "SELECT id, pipeline, trigger_type, status, started_at, finished_at, duration_ms
             FROM pipeline_runs ORDER BY started_at DESC LIMIT ?1"
        )?;
        let rows = stmt.query_map(params![limit], |row| {
            Ok(PipelineRunRow {
                id: row.get(0)?,
                pipeline: row.get(1)?,
                trigger_type: row.get(2)?,
                status: row.get(3)?,
                started_at: row.get(4)?,
                finished_at: row.get(5)?,
                duration_ms: row.get(6)?,
            })
        })?;
        rows.collect()
    }

    /// Get task runs for a specific pipeline run.
    pub fn task_runs_for(&self, run_id: &str) -> Result<Vec<TaskRunRow>, rusqlite::Error> {
        let mut stmt = self.conn.prepare(
            "SELECT id, run_id, task_name, status, started_at, finished_at, duration_ms, error
             FROM task_runs WHERE run_id = ?1 ORDER BY started_at"
        )?;
        let rows = stmt.query_map(params![run_id], |row| {
            Ok(TaskRunRow {
                id: row.get(0)?,
                run_id: row.get(1)?,
                task_name: row.get(2)?,
                status: row.get(3)?,
                started_at: row.get(4)?,
                finished_at: row.get(5)?,
                duration_ms: row.get(6)?,
                error: row.get(7)?,
            })
        })?;
        rows.collect()
    }
}

#[derive(Debug)]
pub struct PipelineRunRow {
    pub id: String,
    pub pipeline: String,
    pub trigger_type: String,
    pub status: String,
    pub started_at: String,
    pub finished_at: Option<String>,
    pub duration_ms: Option<i64>,
}

#[derive(Debug)]
pub struct TaskRunRow {
    pub id: String,
    pub run_id: String,
    pub task_name: String,
    pub status: String,
    pub started_at: String,
    pub finished_at: Option<String>,
    pub duration_ms: Option<i64>,
    pub error: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_and_complete_pipeline_run() {
        let store = Store::in_memory().unwrap();

        store.insert_pipeline_run("run-1", "weather_etl", "interval", "2026-02-03T10:00:00Z").unwrap();

        let runs = store.recent_runs(10).unwrap();
        assert_eq!(runs.len(), 1);
        assert_eq!(runs[0].status, "running");

        store.complete_pipeline_run("run-1", "completed", "2026-02-03T10:00:05Z", 5000).unwrap();

        let runs = store.recent_runs(10).unwrap();
        assert_eq!(runs[0].status, "completed");
        assert_eq!(runs[0].duration_ms, Some(5000));
    }

    #[test]
    fn insert_and_query_task_runs() {
        let store = Store::in_memory().unwrap();

        store.insert_pipeline_run("run-1", "weather_etl", "interval", "2026-02-03T10:00:00Z").unwrap();

        store.insert_task_run(
            "task-1", "run-1", "fetch", "completed",
            "2026-02-03T10:00:00Z", Some("2026-02-03T10:00:02Z"),
            Some(2000), None,
        ).unwrap();

        store.insert_task_run(
            "task-2", "run-1", "save", "failed",
            "2026-02-03T10:00:02Z", Some("2026-02-03T10:00:03Z"),
            Some(1000), Some("disk full"),
        ).unwrap();

        let tasks = store.task_runs_for("run-1").unwrap();
        assert_eq!(tasks.len(), 2);
        assert_eq!(tasks[0].task_name, "fetch");
        assert_eq!(tasks[1].task_name, "save");
        assert_eq!(tasks[1].error.as_deref(), Some("disk full"));
    }

    #[test]
    fn mark_inflight_as_crashed() {
        let store = Store::in_memory().unwrap();

        store.insert_pipeline_run("run-1", "a", "interval", "2026-02-03T10:00:00Z").unwrap();
        store.insert_pipeline_run("run-2", "b", "webhook", "2026-02-03T10:00:01Z").unwrap();
        store.complete_pipeline_run("run-2", "completed", "2026-02-03T10:00:05Z", 4000).unwrap();

        let crashed = store.mark_inflight_as_crashed().unwrap();
        assert_eq!(crashed, 1); // only run-1 was still running

        let runs = store.recent_runs(10).unwrap();
        let run1 = runs.iter().find(|r| r.id == "run-1").unwrap();
        assert_eq!(run1.status, "crashed");

        let run2 = runs.iter().find(|r| r.id == "run-2").unwrap();
        assert_eq!(run2.status, "completed");
    }

    #[test]
    fn recent_runs_respects_limit() {
        let store = Store::in_memory().unwrap();

        for i in 0..5 {
            store.insert_pipeline_run(
                &format!("run-{i}"), "test", "interval",
                &format!("2026-02-03T10:00:{i:02}Z"),
            ).unwrap();
        }

        let runs = store.recent_runs(3).unwrap();
        assert_eq!(runs.len(), 3);
        // Most recent first
        assert_eq!(runs[0].id, "run-4");
    }
}
