use crate::domain::{
    Agent, AgentAvailability, AgentHealth, AgentId, Allocation, AllocationId, AllocationStatus,
    Job, JobId, JobSpec, JobStatus, TerminalResult, Timestamp,
};
use serde::Serialize;
use std::num::NonZeroU32;

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct JobSnapshot {
    pub id: JobId,
    pub spec: JobSpec,
    pub status: JobStatus,
    pub submitted_at: Timestamp,
    pub updated_at: Timestamp,
    pub started_at: Option<Timestamp>,
    pub finished_at: Option<Timestamp>,
    pub terminal_result: Option<TerminalResult>,
}

impl From<&Job> for JobSnapshot {
    fn from(job: &Job) -> Self {
        Self {
            id: job.id(),
            spec: job.spec().clone(),
            status: job.status(),
            submitted_at: job.submitted_at(),
            updated_at: job.updated_at(),
            started_at: job.started_at(),
            finished_at: job.finished_at(),
            terminal_result: job.terminal_result(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct AgentSnapshot {
    pub id: AgentId,
    pub address: String,
    pub last_heartbeat_at: Timestamp,
    pub health: AgentHealth,
    pub availability: AgentAvailability,
}

impl From<&Agent> for AgentSnapshot {
    fn from(agent: &Agent) -> Self {
        Self {
            id: agent.id(),
            address: agent.address().to_owned(),
            last_heartbeat_at: agent.last_heartbeat_at(),
            health: agent.health(),
            availability: agent.availability(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct AllocationSnapshot {
    pub id: AllocationId,
    pub job_id: JobId,
    pub agent_id: AgentId,
    pub status: AllocationStatus,
    pub attempt: NonZeroU32,
    pub assigned_at: Timestamp,
    pub started_at: Option<Timestamp>,
    pub finished_at: Option<Timestamp>,
}

impl From<&Allocation> for AllocationSnapshot {
    fn from(allocation: &Allocation) -> Self {
        Self {
            id: allocation.id(),
            job_id: allocation.job_id(),
            agent_id: allocation.agent_id(),
            status: allocation.status(),
            attempt: allocation.attempt(),
            assigned_at: allocation.assigned_at(),
            started_at: allocation.started_at(),
            finished_at: allocation.finished_at(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{AgentSnapshot, AllocationSnapshot, JobSnapshot};
    use crate::domain::{
        Agent, AgentAvailability, AgentHealth, AgentId, Allocation, AllocationId, AllocationStatus,
        ApplyOutcome, Job, JobEvent, JobId, JobSpec, ProcessResult, Timestamp, apply_event,
    };
    use serde_json::{Value, json};
    use std::{collections::BTreeMap, num::NonZeroU32};

    #[test]
    fn job_snapshot_serializes_all_fields_without_consuming_source() {
        let id = "550e8400-e29b-41d4-a716-446655440000"
            .parse::<JobId>()
            .unwrap();
        let submitted_at = Timestamp::from_unix_millis(1_742_000_000_123);
        let spec = JobSpec::new(
            "echo",
            vec!["hello".into()],
            Some(BTreeMap::from([("MODE".into(), "fast".into())])),
            Some("/work".into()),
        )
        .unwrap();
        let job = Job::new(id, spec, submitted_at);

        let snapshot = JobSnapshot::from(&job);
        let value = serde_json::to_value(&snapshot).unwrap();

        assert_eq!(
            value,
            json!({
                "id": id.to_string(),
                "spec": {
                    "command": "echo",
                    "args": ["hello"],
                    "env": {"MODE": "fast"},
                    "working_dir": "/work"
                },
                "status": "pending",
                "submitted_at": 1_742_000_000_123_u64,
                "updated_at": 1_742_000_000_123_u64,
                "started_at": null,
                "finished_at": null,
                "terminal_result": null
            })
        );
        assert_eq!(job.id(), id);
        assert_eq!(job.spec().command(), "echo");
    }

    #[test]
    fn terminal_snapshots_serialize_public_fields_without_internal_allocation_context() {
        let job_id = "550e8400-e29b-41d4-a716-446655440000"
            .parse::<JobId>()
            .unwrap();
        let allocation_id = "550e8400-e29b-41d4-a716-446655440002"
            .parse::<AllocationId>()
            .unwrap();
        let agent_id = "550e8400-e29b-41d4-a716-446655440001"
            .parse::<AgentId>()
            .unwrap();
        let submitted_at = Timestamp::from_unix_millis(1_742_000_000_100);
        let assigned_at = Timestamp::from_unix_millis(1_742_000_000_110);
        let started_at = Timestamp::from_unix_millis(1_742_000_000_120);
        let finished_at = Timestamp::from_unix_millis(1_742_000_000_130);
        let spec = JobSpec::new(
            "echo",
            vec!["hello".into()],
            Some(BTreeMap::from([("MODE".into(), "fast".into())])),
            Some("/work".into()),
        )
        .unwrap();
        let mut job = Job::new(job_id, spec, submitted_at);
        let mut allocation = Allocation::new(
            allocation_id,
            job_id,
            agent_id,
            NonZeroU32::new(3).unwrap(),
            assigned_at,
        );
        let events = [
            JobEvent::Assigned {
                job_id,
                allocation_id,
                agent_id,
                occurred_at: assigned_at,
            },
            JobEvent::Started {
                job_id,
                allocation_id,
                agent_id,
                occurred_at: started_at,
            },
            JobEvent::Finished {
                job_id,
                allocation_id,
                agent_id,
                occurred_at: finished_at,
                result: ProcessResult::Exited { exit_code: 0 },
            },
        ];

        for event in &events {
            assert_eq!(
                apply_event(&mut job, Some(&mut allocation), event).unwrap(),
                ApplyOutcome::Applied
            );
        }

        assert_eq!(
            serde_json::to_value(JobSnapshot::from(&job)).unwrap(),
            json!({
                "id": job_id.to_string(),
                "spec": {
                    "command": "echo",
                    "args": ["hello"],
                    "env": {"MODE": "fast"},
                    "working_dir": "/work"
                },
                "status": "succeeded",
                "submitted_at": 1_742_000_000_100_u64,
                "updated_at": 1_742_000_000_130_u64,
                "started_at": 1_742_000_000_120_u64,
                "finished_at": 1_742_000_000_130_u64,
                "terminal_result": {"exited": {"exit_code": 0}}
            })
        );
        assert_eq!(
            serde_json::to_value(AllocationSnapshot::from(&allocation)).unwrap(),
            json!({
                "id": allocation_id.to_string(),
                "job_id": job_id.to_string(),
                "agent_id": agent_id.to_string(),
                "status": "succeeded",
                "attempt": 3,
                "assigned_at": 1_742_000_000_110_u64,
                "started_at": 1_742_000_000_120_u64,
                "finished_at": 1_742_000_000_130_u64
            })
        );
    }

    #[test]
    fn agent_snapshot_serializes_all_fields_without_consuming_source() {
        let id = "550e8400-e29b-41d4-a716-446655440001"
            .parse::<AgentId>()
            .unwrap();
        let last_heartbeat_at = Timestamp::from_unix_millis(1_742_000_000_456);
        let agent = Agent::new(id, "worker.example:9000".into(), last_heartbeat_at);

        let snapshot = AgentSnapshot::from(&agent);
        let value: Value = serde_json::to_value(&snapshot).unwrap();

        assert_eq!(
            value,
            json!({
                "id": id.to_string(),
                "address": "worker.example:9000",
                "last_heartbeat_at": 1_742_000_000_456_u64,
                "health": "healthy",
                "availability": "idle"
            })
        );
        assert_eq!(agent.id(), id);
        assert_eq!(agent.address(), "worker.example:9000");
        assert_eq!(agent.health(), AgentHealth::Healthy);
        assert_eq!(agent.availability(), AgentAvailability::Idle);
    }

    #[test]
    fn allocation_snapshot_serializes_all_fields_without_consuming_source() {
        let id = "550e8400-e29b-41d4-a716-446655440002"
            .parse::<AllocationId>()
            .unwrap();
        let job_id = "550e8400-e29b-41d4-a716-446655440000"
            .parse::<JobId>()
            .unwrap();
        let agent_id = "550e8400-e29b-41d4-a716-446655440001"
            .parse::<AgentId>()
            .unwrap();
        let attempt = NonZeroU32::new(7).unwrap();
        let assigned_at = Timestamp::from_unix_millis(1_742_000_000_789);
        let allocation = Allocation::new(id, job_id, agent_id, attempt, assigned_at);

        let snapshot = AllocationSnapshot::from(&allocation);
        let value = serde_json::to_value(&snapshot).unwrap();

        assert_eq!(
            value,
            json!({
                "id": id.to_string(),
                "job_id": job_id.to_string(),
                "agent_id": agent_id.to_string(),
                "status": "assigned",
                "attempt": 7,
                "assigned_at": 1_742_000_000_789_u64,
                "started_at": null,
                "finished_at": null
            })
        );
        assert_eq!(allocation.id(), id);
        assert_eq!(allocation.job_id(), job_id);
        assert_eq!(allocation.agent_id(), agent_id);
        assert_eq!(allocation.status(), AllocationStatus::Assigned);
        assert_eq!(allocation.attempt(), attempt);
        assert_eq!(allocation.assigned_at(), assigned_at);
        assert_eq!(allocation.started_at(), None);
        assert_eq!(allocation.finished_at(), None);
    }
}
