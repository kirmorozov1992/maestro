use crate::domain::{JobId, JobSpec, Timestamp};
use serde::Serialize;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Job {
    id: JobId,
    spec: JobSpec,
    status: JobStatus,
    submitted_at: Timestamp,
    updated_at: Timestamp,
    started_at: Option<Timestamp>,
    finished_at: Option<Timestamp>,
    terminal_result: Option<TerminalResult>,
}

impl Job {
    pub fn new(id: JobId, spec: JobSpec, submitted_at: Timestamp) -> Self {
        Self {
            id,
            spec,
            status: JobStatus::Pending,
            submitted_at,
            updated_at: submitted_at,
            started_at: None,
            finished_at: None,
            terminal_result: None,
        }
    }

    pub fn id(&self) -> JobId {
        self.id
    }

    pub fn spec(&self) -> &JobSpec {
        &self.spec
    }

    pub fn status(&self) -> JobStatus {
        self.status
    }

    pub fn submitted_at(&self) -> Timestamp {
        self.submitted_at
    }

    pub fn updated_at(&self) -> Timestamp {
        self.updated_at
    }

    pub fn started_at(&self) -> Option<Timestamp> {
        self.started_at
    }

    pub fn finished_at(&self) -> Option<Timestamp> {
        self.finished_at
    }

    pub fn terminal_result(&self) -> Option<TerminalResult> {
        self.terminal_result
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum JobStatus {
    Pending,
    Assigned,
    Running,
    Succeeded,
    Failed,
    Stopped,
    Lost,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TerminalResult {
    Exited { exit_code: i32 },
    SpawnFailed,
    Stopped,
    Lost,
}

#[cfg(test)]
mod tests {
    use super::{Job, JobStatus, TerminalResult};
    use crate::domain::{JobId, JobSpec, Timestamp};

    #[test]
    fn job_starts_pending_with_explicit_submission_times() {
        let submitted_at = Timestamp::from_unix_millis(1_742_000_000_123);
        let spec = JobSpec::new("echo", vec!["hello".into()], None, None)
            .expect("valid job spec should be accepted");
        let job = Job::new(JobId::new(), spec, submitted_at);

        assert_eq!(job.status(), JobStatus::Pending);
        assert_eq!(job.submitted_at(), submitted_at);
        assert_eq!(job.updated_at(), submitted_at);
        assert_eq!(job.started_at(), None);
        assert_eq!(job.finished_at(), None);
        assert_eq!(job.terminal_result(), None);
    }

    #[test]
    fn job_status_and_terminal_results_use_snake_case_serde_names() {
        assert_eq!(
            serde_json::to_string(&JobStatus::Pending).unwrap(),
            "\"pending\""
        );
        assert_eq!(
            serde_json::to_string(&JobStatus::Assigned).unwrap(),
            "\"assigned\""
        );
        assert_eq!(
            serde_json::to_string(&JobStatus::Running).unwrap(),
            "\"running\""
        );
        assert_eq!(
            serde_json::to_string(&JobStatus::Succeeded).unwrap(),
            "\"succeeded\""
        );
        assert_eq!(
            serde_json::to_string(&JobStatus::Failed).unwrap(),
            "\"failed\""
        );
        assert_eq!(
            serde_json::to_string(&JobStatus::Stopped).unwrap(),
            "\"stopped\""
        );
        assert_eq!(serde_json::to_string(&JobStatus::Lost).unwrap(), "\"lost\"");

        assert_eq!(
            serde_json::to_string(&TerminalResult::Exited { exit_code: 2 }).unwrap(),
            "{\"exited\":{\"exit_code\":2}}"
        );
        assert_eq!(
            serde_json::to_string(&TerminalResult::SpawnFailed).unwrap(),
            "\"spawn_failed\""
        );
        assert_eq!(
            serde_json::to_string(&TerminalResult::Stopped).unwrap(),
            "\"stopped\""
        );
        assert_eq!(
            serde_json::to_string(&TerminalResult::Lost).unwrap(),
            "\"lost\""
        );
    }
}
