use crate::domain::{AgentId, AllocationId, JobId, Timestamp};
use serde::Serialize;
use std::num::NonZeroU32;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Allocation {
    id: AllocationId,
    job_id: JobId,
    agent_id: AgentId,
    status: AllocationStatus,
    attempt: NonZeroU32,
    assigned_at: Timestamp,
    started_at: Option<Timestamp>,
    finished_at: Option<Timestamp>,
}

impl Allocation {
    pub fn new(
        id: AllocationId,
        job_id: JobId,
        agent_id: AgentId,
        attempt: NonZeroU32,
        assigned_at: Timestamp,
    ) -> Self {
        Self {
            id,
            job_id,
            agent_id,
            status: AllocationStatus::Assigned,
            attempt,
            assigned_at,
            started_at: None,
            finished_at: None,
        }
    }

    pub fn id(&self) -> AllocationId {
        self.id
    }

    pub fn job_id(&self) -> JobId {
        self.job_id
    }

    pub fn agent_id(&self) -> AgentId {
        self.agent_id
    }

    pub fn status(&self) -> AllocationStatus {
        self.status
    }

    pub fn attempt(&self) -> NonZeroU32 {
        self.attempt
    }

    pub fn assigned_at(&self) -> Timestamp {
        self.assigned_at
    }

    pub fn started_at(&self) -> Option<Timestamp> {
        self.started_at
    }

    pub fn finished_at(&self) -> Option<Timestamp> {
        self.finished_at
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AllocationStatus {
    Assigned,
    Running,
    Succeeded,
    Failed,
    Stopped,
    Lost,
}

#[cfg(test)]
mod tests {
    use super::{Allocation, AllocationStatus};
    use crate::domain::{AgentId, AllocationId, JobId, Timestamp};
    use std::num::NonZeroU32;

    #[test]
    fn allocation_starts_assigned_with_explicit_links_and_attempt() {
        let id = AllocationId::new();
        let job_id = JobId::new();
        let agent_id = AgentId::new();
        let attempt = NonZeroU32::new(7).unwrap();
        let assigned_at = Timestamp::from_unix_millis(1_742_000_000_789);
        let allocation = Allocation::new(id, job_id, agent_id, attempt, assigned_at);

        assert_eq!(allocation.id(), id);
        assert_eq!(allocation.job_id(), job_id);
        assert_eq!(allocation.agent_id(), agent_id);
        assert_eq!(allocation.status(), AllocationStatus::Assigned);
        assert_eq!(allocation.attempt(), attempt);
        assert_eq!(allocation.assigned_at(), assigned_at);
        assert_eq!(allocation.started_at(), None);
        assert_eq!(allocation.finished_at(), None);
        assert!(NonZeroU32::new(0).is_none());
    }

    #[test]
    fn allocation_status_uses_snake_case_serde_names() {
        assert_eq!(
            serde_json::to_string(&AllocationStatus::Assigned).unwrap(),
            "\"assigned\""
        );
        assert_eq!(
            serde_json::to_string(&AllocationStatus::Running).unwrap(),
            "\"running\""
        );
        assert_eq!(
            serde_json::to_string(&AllocationStatus::Succeeded).unwrap(),
            "\"succeeded\""
        );
        assert_eq!(
            serde_json::to_string(&AllocationStatus::Failed).unwrap(),
            "\"failed\""
        );
        assert_eq!(
            serde_json::to_string(&AllocationStatus::Stopped).unwrap(),
            "\"stopped\""
        );
        assert_eq!(
            serde_json::to_string(&AllocationStatus::Lost).unwrap(),
            "\"lost\""
        );
    }
}
