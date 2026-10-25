use crate::{
    application::{AgentAssignment, JobSubmission, StopOutcome},
    domain::{
        AgentAvailability as DomainAgentAvailability, AgentHealth as DomainAgentHealth, AgentId,
        AgentSnapshot, AllocationId, AllocationSnapshot,
        AllocationStatus as DomainAllocationStatus, ApplyOutcome, JobEvent, JobId, JobSnapshot,
        JobSpec, JobSpecError, JobStatus as DomainJobStatus, ProcessResult, StopReason,
        TerminalResult, Timestamp,
    },
    error::AppError,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Clone, Debug, Eq, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CreateJobRequest {
    pub(crate) command: String,
    #[serde(default)]
    pub(crate) args: Vec<String>,
    pub(crate) env: Option<BTreeMap<String, String>>,
    pub(crate) working_dir: Option<String>,
}

impl CreateJobRequest {
    pub(crate) fn into_submission(self) -> JobSubmission {
        JobSubmission {
            command: self.command,
            args: self.args,
            env: self.env,
            working_dir: self.working_dir,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RegisterAgentRequest {
    pub(crate) address: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum ProcessResultV1 {
    Exited { exit_code: i32 },
    SpawnFailed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum StopReasonV1 {
    UserRequested,
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum AllocationEventRequest {
    Started {
        job_id: JobId,
        agent_id: AgentId,
        occurred_at: Timestamp,
    },
    Finished {
        job_id: JobId,
        agent_id: AgentId,
        occurred_at: Timestamp,
        result: ProcessResultV1,
    },
    Stopped {
        job_id: JobId,
        agent_id: AgentId,
        occurred_at: Timestamp,
        reason: StopReasonV1,
    },
}

impl AllocationEventRequest {
    pub(crate) fn into_domain_event(self, allocation_id: AllocationId) -> JobEvent {
        match self {
            Self::Started {
                job_id,
                agent_id,
                occurred_at,
            } => JobEvent::Started {
                job_id,
                allocation_id,
                agent_id,
                occurred_at,
            },
            Self::Finished {
                job_id,
                agent_id,
                occurred_at,
                result,
            } => JobEvent::Finished {
                job_id,
                allocation_id,
                agent_id,
                occurred_at,
                result: match result {
                    ProcessResultV1::Exited { exit_code } => ProcessResult::Exited { exit_code },
                    ProcessResultV1::SpawnFailed => ProcessResult::SpawnFailed,
                },
            },
            Self::Stopped {
                job_id,
                agent_id,
                occurred_at,
                reason,
            } => JobEvent::Stopped {
                job_id,
                allocation_id: Some(allocation_id),
                agent_id: Some(agent_id),
                occurred_at,
                reason: match reason {
                    StopReasonV1::UserRequested => StopReason::UserRequested,
                },
            },
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum JobStatusV1 {
    Pending,
    Assigned,
    Running,
    Succeeded,
    Failed,
    Stopped,
    Lost,
}

impl From<DomainJobStatus> for JobStatusV1 {
    fn from(status: DomainJobStatus) -> Self {
        match status {
            DomainJobStatus::Pending => Self::Pending,
            DomainJobStatus::Assigned => Self::Assigned,
            DomainJobStatus::Running => Self::Running,
            DomainJobStatus::Succeeded => Self::Succeeded,
            DomainJobStatus::Failed => Self::Failed,
            DomainJobStatus::Stopped => Self::Stopped,
            DomainJobStatus::Lost => Self::Lost,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum AllocationStatusV1 {
    Assigned,
    Running,
    Succeeded,
    Failed,
    Stopped,
    Lost,
}

impl From<DomainAllocationStatus> for AllocationStatusV1 {
    fn from(status: DomainAllocationStatus) -> Self {
        match status {
            DomainAllocationStatus::Assigned => Self::Assigned,
            DomainAllocationStatus::Running => Self::Running,
            DomainAllocationStatus::Succeeded => Self::Succeeded,
            DomainAllocationStatus::Failed => Self::Failed,
            DomainAllocationStatus::Stopped => Self::Stopped,
            DomainAllocationStatus::Lost => Self::Lost,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum AgentHealthV1 {
    Healthy,
    Unhealthy,
}

impl From<DomainAgentHealth> for AgentHealthV1 {
    fn from(health: DomainAgentHealth) -> Self {
        match health {
            DomainAgentHealth::Healthy => Self::Healthy,
            DomainAgentHealth::Unhealthy => Self::Unhealthy,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum AgentAvailabilityV1 {
    Idle,
    Busy,
}

impl From<DomainAgentAvailability> for AgentAvailabilityV1 {
    fn from(availability: DomainAgentAvailability) -> Self {
        match availability {
            DomainAgentAvailability::Idle => Self::Idle,
            DomainAgentAvailability::Busy => Self::Busy,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub(crate) enum TerminalResultV1 {
    Exited { exit_code: i32 },
    SpawnFailed,
    Stopped,
    Lost,
}

impl From<TerminalResult> for TerminalResultV1 {
    fn from(result: TerminalResult) -> Self {
        match result {
            TerminalResult::Exited { exit_code } => Self::Exited { exit_code },
            TerminalResult::SpawnFailed => Self::SpawnFailed,
            TerminalResult::Stopped => Self::Stopped,
            TerminalResult::Lost => Self::Lost,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub(crate) struct JobSpecResponse {
    pub(crate) command: String,
    pub(crate) args: Vec<String>,
    pub(crate) env_keys: Option<Vec<String>>,
    pub(crate) working_dir: Option<String>,
}

impl From<&JobSpec> for JobSpecResponse {
    fn from(spec: &JobSpec) -> Self {
        Self {
            command: spec.command().to_owned(),
            args: spec.args().to_vec(),
            env_keys: spec
                .env()
                .map(|environment| environment.keys().cloned().collect()),
            working_dir: spec.working_dir().map(str::to_owned),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub(crate) struct JobDataResponse {
    pub(crate) id: JobId,
    pub(crate) spec: JobSpecResponse,
    pub(crate) status: JobStatusV1,
    pub(crate) submitted_at: Timestamp,
    pub(crate) updated_at: Timestamp,
    pub(crate) started_at: Option<Timestamp>,
    pub(crate) finished_at: Option<Timestamp>,
    pub(crate) terminal_result: Option<TerminalResultV1>,
}

impl From<&JobSnapshot> for JobDataResponse {
    fn from(job: &JobSnapshot) -> Self {
        Self {
            id: job.id,
            spec: JobSpecResponse::from(&job.spec),
            status: JobStatusV1::from(job.status),
            submitted_at: job.submitted_at,
            updated_at: job.updated_at,
            started_at: job.started_at,
            finished_at: job.finished_at,
            terminal_result: job.terminal_result.map(TerminalResultV1::from),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub(crate) struct AllocationResponse {
    pub(crate) id: AllocationId,
    pub(crate) job_id: JobId,
    pub(crate) agent_id: AgentId,
    pub(crate) status: AllocationStatusV1,
    pub(crate) attempt: u32,
    pub(crate) assigned_at: Timestamp,
    pub(crate) started_at: Option<Timestamp>,
    pub(crate) finished_at: Option<Timestamp>,
}

impl From<&AllocationSnapshot> for AllocationResponse {
    fn from(allocation: &AllocationSnapshot) -> Self {
        Self {
            id: allocation.id,
            job_id: allocation.job_id,
            agent_id: allocation.agent_id,
            status: AllocationStatusV1::from(allocation.status),
            attempt: allocation.attempt.get(),
            assigned_at: allocation.assigned_at,
            started_at: allocation.started_at,
            finished_at: allocation.finished_at,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub(crate) struct JobResponse {
    #[serde(flatten)]
    pub(crate) job: JobDataResponse,
    pub(crate) allocation: Option<AllocationResponse>,
}

impl JobResponse {
    pub(crate) fn from_parts(job: &JobSnapshot, allocation: Option<&AllocationSnapshot>) -> Self {
        Self {
            job: JobDataResponse::from(job),
            allocation: allocation.map(AllocationResponse::from),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub(crate) struct JobListResponse {
    pub(crate) items: Vec<JobResponse>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub(crate) struct AgentResponse {
    pub(crate) id: AgentId,
    pub(crate) address: String,
    pub(crate) last_heartbeat_at: Timestamp,
    pub(crate) health: AgentHealthV1,
    pub(crate) availability: AgentAvailabilityV1,
}

impl From<&AgentSnapshot> for AgentResponse {
    fn from(agent: &AgentSnapshot) -> Self {
        Self {
            id: agent.id,
            address: agent.address.clone(),
            last_heartbeat_at: agent.last_heartbeat_at,
            health: AgentHealthV1::from(agent.health),
            availability: AgentAvailabilityV1::from(agent.availability),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub(crate) struct AssignmentResponse {
    pub(crate) job: JobDataResponse,
    pub(crate) allocation: AllocationResponse,
}

impl From<&AgentAssignment> for AssignmentResponse {
    fn from(assignment: &AgentAssignment) -> Self {
        Self {
            job: JobDataResponse::from(&assignment.job),
            allocation: AllocationResponse::from(&assignment.allocation),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(tag = "outcome", rename_all = "snake_case")]
pub(crate) enum StopJobResponse {
    Completed {
        job: Box<JobResponse>,
    },
    Requested {
        job_id: JobId,
        allocation_id: AllocationId,
        agent_id: AgentId,
    },
}

impl From<StopOutcome> for StopJobResponse {
    fn from(outcome: StopOutcome) -> Self {
        match outcome {
            StopOutcome::Completed(job) => Self::Completed {
                job: Box::new(JobResponse::from_parts(&job, None)),
            },
            StopOutcome::Requested(request) => Self::Requested {
                job_id: request.job_id,
                allocation_id: request.allocation_id,
                agent_id: request.agent_id,
            },
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum EventOutcomeV1 {
    Applied,
    Duplicate,
    Noop,
}

impl From<ApplyOutcome> for EventOutcomeV1 {
    fn from(outcome: ApplyOutcome) -> Self {
        match outcome {
            ApplyOutcome::Applied => Self::Applied,
            ApplyOutcome::Duplicate => Self::Duplicate,
            ApplyOutcome::Noop => Self::Noop,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub(crate) struct EventReportResponse {
    pub(crate) outcome: EventOutcomeV1,
}

impl From<ApplyOutcome> for EventReportResponse {
    fn from(outcome: ApplyOutcome) -> Self {
        Self {
            outcome: EventOutcomeV1::from(outcome),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum HealthStatus {
    Ok,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub(crate) struct HealthResponse {
    pub(crate) status: HealthStatus,
}

impl HealthResponse {
    pub(crate) const fn ok() -> Self {
        Self {
            status: HealthStatus::Ok,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub(crate) struct FieldError {
    pub(crate) field: FieldNameV1,
    pub(crate) code: FieldErrorCodeV1,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum FieldNameV1 {
    Command,
    Args,
    Env,
    WorkingDir,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum FieldErrorCodeV1 {
    EmptyCommand,
    EmptyWorkingDirectory,
    CommandTooLong,
    ArgumentsTooLong,
    EnvironmentTooLong,
    WorkingDirectoryTooLong,
}

impl From<JobSpecError> for FieldError {
    fn from(error: JobSpecError) -> Self {
        let (field, code) = match error {
            JobSpecError::EmptyCommand => (FieldNameV1::Command, FieldErrorCodeV1::EmptyCommand),
            JobSpecError::EmptyWorkingDirectory => (
                FieldNameV1::WorkingDir,
                FieldErrorCodeV1::EmptyWorkingDirectory,
            ),
            JobSpecError::CommandTooLong { .. } => {
                (FieldNameV1::Command, FieldErrorCodeV1::CommandTooLong)
            }
            JobSpecError::ArgumentsTooLong { .. } => {
                (FieldNameV1::Args, FieldErrorCodeV1::ArgumentsTooLong)
            }
            JobSpecError::EnvironmentTooLong { .. } => {
                (FieldNameV1::Env, FieldErrorCodeV1::EnvironmentTooLong)
            }
            JobSpecError::WorkingDirectoryTooLong { .. } => (
                FieldNameV1::WorkingDir,
                FieldErrorCodeV1::WorkingDirectoryTooLong,
            ),
        };
        Self { field, code }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub(crate) struct ErrorDetails {
    pub(crate) field_errors: Vec<FieldError>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub(crate) struct ErrorResponse {
    pub(crate) code: String,
    pub(crate) message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) details: Option<ErrorDetails>,
}

impl From<&AppError> for ErrorResponse {
    fn from(error: &AppError) -> Self {
        let public = error.public_error();
        Self {
            code: public.code.to_owned(),
            message: public.message.to_owned(),
            details: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        AgentResponse, AllocationEventRequest, CreateJobRequest, ErrorDetails, ErrorResponse,
        EventReportResponse, FieldError, HealthResponse, HealthStatus, JobListResponse,
        JobResponse, JobStatusV1, RegisterAgentRequest, StopJobResponse, StopReasonV1,
    };
    use crate::{
        application::{AgentAssignment, JobSubmission, StopOutcome, StopRequest},
        domain::{
            Agent, AgentId, AgentSnapshot, Allocation, AllocationId, AllocationSnapshot,
            ApplyOutcome, Job, JobEvent, JobId, JobSnapshot, JobSpec, JobSpecError, ProcessResult,
            StopReason, Timestamp,
        },
        error::AppError,
    };
    use serde_json::{Value, json};
    use std::{collections::BTreeMap, num::NonZeroU32};

    fn job_id() -> JobId {
        "550e8400-e29b-41d4-a716-446655440000".parse().unwrap()
    }

    fn agent_id() -> AgentId {
        "550e8400-e29b-41d4-a716-446655440001".parse().unwrap()
    }

    fn allocation_id() -> AllocationId {
        "550e8400-e29b-41d4-a716-446655440002".parse().unwrap()
    }

    #[test]
    fn create_job_request_requires_command_defaults_args_and_rejects_unknown_fields() {
        let request: CreateJobRequest = serde_json::from_value(json!({
            "command": "echo"
        }))
        .unwrap();

        assert_eq!(request.command, "echo");
        assert!(request.args.is_empty());
        assert_eq!(request.env, None);
        assert_eq!(request.working_dir, None);

        let submission: JobSubmission = request.into_submission();
        assert_eq!(submission.command, "echo");
        assert!(submission.args.is_empty());
        assert_eq!(submission.env, None);
        assert_eq!(submission.working_dir, None);

        assert!(serde_json::from_value::<CreateJobRequest>(json!({})).is_err());
        assert!(
            serde_json::from_value::<CreateJobRequest>(json!({
                "command": "echo",
                "unknown": true
            }))
            .is_err()
        );
        assert!(
            serde_json::from_value::<CreateJobRequest>(json!({
                "command": "echo",
                "args": null
            }))
            .is_err()
        );
    }

    #[test]
    fn create_job_request_accepts_null_optional_fields_and_preserves_empty_env() {
        let absent: CreateJobRequest = serde_json::from_value(json!({
            "command": "echo",
            "env": null,
            "working_dir": null
        }))
        .unwrap();
        assert_eq!(absent.env, None);
        assert_eq!(absent.working_dir, None);

        let empty_env: CreateJobRequest = serde_json::from_value(json!({
            "command": "echo",
            "env": {}
        }))
        .unwrap();
        assert_eq!(empty_env.env, Some(BTreeMap::new()));
    }

    #[test]
    fn register_agent_request_requires_address_and_rejects_client_supplied_id() {
        let request: RegisterAgentRequest = serde_json::from_value(json!({
            "address": "worker.example:9000"
        }))
        .unwrap();
        assert_eq!(request.address, "worker.example:9000");
        assert!(serde_json::from_value::<RegisterAgentRequest>(json!({})).is_err());
        assert!(
            serde_json::from_value::<RegisterAgentRequest>(json!({
                "address": "worker.example:9000",
                "agent_id": agent_id()
            }))
            .is_err()
        );
    }

    #[test]
    fn allocation_event_request_uses_path_id_and_maps_finished_result() {
        let request: AllocationEventRequest = serde_json::from_value(json!({
            "type": "finished",
            "job_id": job_id(),
            "agent_id": agent_id(),
            "occurred_at": 1_742_000_000_123_u64,
            "result": {"type": "exited", "exit_code": 7}
        }))
        .unwrap();
        let event = request.into_domain_event(allocation_id());

        assert_eq!(
            event,
            JobEvent::Finished {
                job_id: job_id(),
                allocation_id: allocation_id(),
                agent_id: agent_id(),
                occurred_at: Timestamp::from_unix_millis(1_742_000_000_123),
                result: ProcessResult::Exited { exit_code: 7 },
            }
        );
    }

    #[test]
    fn allocation_event_request_rejects_server_events_unknown_fields_and_missing_values() {
        for event_type in ["submitted", "assigned", "lost"] {
            assert!(
                serde_json::from_value::<AllocationEventRequest>(json!({
                    "type": event_type,
                    "job_id": job_id(),
                    "agent_id": agent_id(),
                    "occurred_at": 123
                }))
                .is_err()
            );
        }
        assert!(
            serde_json::from_value::<AllocationEventRequest>(json!({
                "type": "started",
                "job_id": job_id(),
                "agent_id": agent_id(),
                "occurred_at": 123,
                "allocation_id": allocation_id()
            }))
            .is_err()
        );
        assert!(
            serde_json::from_value::<AllocationEventRequest>(json!({
                "type": "started",
                "job_id": job_id(),
                "agent_id": agent_id()
            }))
            .is_err()
        );
    }

    #[test]
    fn stopped_event_requires_agent_context_and_maps_reason() {
        let request: AllocationEventRequest = serde_json::from_value(json!({
            "type": "stopped",
            "job_id": job_id(),
            "agent_id": agent_id(),
            "occurred_at": 123,
            "reason": "user_requested"
        }))
        .unwrap();
        assert_eq!(
            request.into_domain_event(allocation_id()),
            JobEvent::Stopped {
                job_id: job_id(),
                allocation_id: Some(allocation_id()),
                agent_id: Some(agent_id()),
                occurred_at: Timestamp::from_unix_millis(123),
                reason: StopReason::UserRequested,
            }
        );
        assert!(
            serde_json::from_value::<AllocationEventRequest>(json!({
                "type": "stopped",
                "job_id": job_id(),
                "occurred_at": 123,
                "reason": "user_requested"
            }))
            .is_err()
        );
        assert_eq!(
            serde_json::from_str::<StopReasonV1>("\"user_requested\"").unwrap(),
            StopReasonV1::UserRequested
        );
    }

    #[test]
    fn job_response_projects_environment_keys_but_never_values() {
        let job = Job::new(
            job_id(),
            JobSpec::new(
                "echo",
                vec!["hello".into()],
                Some(BTreeMap::from([
                    ("TOKEN".into(), "sensitive-value".into()),
                    ("MODE".into(), "fast".into()),
                ])),
                Some("/work".into()),
            )
            .unwrap(),
            Timestamp::from_unix_millis(100),
        );
        let job = JobSnapshot::from(&job);
        let allocation = Allocation::new(
            allocation_id(),
            job_id(),
            agent_id(),
            NonZeroU32::new(1).unwrap(),
            Timestamp::from_unix_millis(110),
        );
        let allocation = AllocationSnapshot::from(&allocation);

        let response = JobResponse::from_parts(&job, Some(&allocation));
        let value: Value = serde_json::to_value(&response).unwrap();
        let encoded = serde_json::to_string(&response).unwrap();

        assert_eq!(value["id"], job_id().to_string());
        assert_eq!(value["status"], "pending");
        assert_eq!(value["spec"]["command"], "echo");
        assert_eq!(value["spec"]["args"], json!(["hello"]));
        assert_eq!(value["spec"]["env_keys"], json!(["MODE", "TOKEN"]));
        assert!(value["spec"].get("env").is_none());
        assert_eq!(value["spec"]["working_dir"], "/work");
        assert_eq!(value["allocation"]["id"], allocation_id().to_string());
        assert_eq!(value["started_at"], Value::Null);
        assert!(!encoded.contains("sensitive-value"));
    }

    #[test]
    fn agent_response_maps_health_and_availability_to_v1_spellings() {
        let agent = Agent::new(
            agent_id(),
            "worker.example:9000".into(),
            Timestamp::from_unix_millis(321),
        );
        let snapshot = AgentSnapshot::from(&agent);
        let value = serde_json::to_value(AgentResponse::from(&snapshot)).unwrap();

        assert_eq!(
            value,
            json!({
                "id": agent_id().to_string(),
                "address": "worker.example:9000",
                "last_heartbeat_at": 321,
                "health": "healthy",
                "availability": "idle"
            })
        );
        assert_eq!(
            serde_json::to_string(&JobStatusV1::Running).unwrap(),
            "\"running\""
        );
    }

    #[test]
    fn stop_response_is_tagged_and_contains_only_the_selected_outcome() {
        let response = StopJobResponse::from(StopOutcome::Requested(StopRequest {
            job_id: job_id(),
            allocation_id: allocation_id(),
            agent_id: agent_id(),
        }));

        assert_eq!(
            serde_json::to_value(response).unwrap(),
            json!({
                "outcome": "requested",
                "job_id": job_id().to_string(),
                "allocation_id": allocation_id().to_string(),
                "agent_id": agent_id().to_string()
            })
        );

        let job = Job::new(
            job_id(),
            JobSpec::new("echo", Vec::new(), None, None).unwrap(),
            Timestamp::from_unix_millis(100),
        );
        let response = StopJobResponse::from(StopOutcome::Completed(JobSnapshot::from(&job)));
        let value = serde_json::to_value(response).unwrap();
        assert_eq!(value["outcome"], "completed");
        assert_eq!(value["job"]["id"], job_id().to_string());
        assert_eq!(value["job"]["allocation"], Value::Null);
    }

    #[test]
    fn assignment_and_event_responses_use_transport_owned_shapes() {
        let job = Job::new(
            job_id(),
            JobSpec::new("echo", Vec::new(), None, None).unwrap(),
            Timestamp::from_unix_millis(100),
        );
        let allocation = Allocation::new(
            allocation_id(),
            job_id(),
            agent_id(),
            NonZeroU32::new(1).unwrap(),
            Timestamp::from_unix_millis(110),
        );
        let assignment = AgentAssignment {
            job: JobSnapshot::from(&job),
            allocation: AllocationSnapshot::from(&allocation),
        };

        let value = serde_json::to_value(super::AssignmentResponse::from(&assignment)).unwrap();
        assert_eq!(value["job"]["status"], "pending");
        assert_eq!(value["allocation"]["status"], "assigned");
        assert_eq!(
            serde_json::to_value(EventReportResponse::from(ApplyOutcome::Duplicate)).unwrap(),
            json!({"outcome": "duplicate"})
        );
    }

    #[test]
    fn health_response_has_stable_ok_shape() {
        assert_eq!(
            serde_json::to_value(HealthResponse::ok()).unwrap(),
            json!({"status": "ok"})
        );
        assert_eq!(HealthStatus::Ok, HealthStatus::Ok);
        assert_eq!(
            serde_json::to_value(JobListResponse { items: Vec::new() }).unwrap(),
            json!({"items": []})
        );
    }

    #[test]
    fn public_error_response_omits_internal_context_and_optional_details() {
        let error = AppError::internal(
            "database password=secret at /private/path",
            std::io::Error::other("private source value"),
        );
        let response = ErrorResponse::from(&error);
        let value = serde_json::to_value(&response).unwrap();
        let encoded = serde_json::to_string(&response).unwrap();

        assert_eq!(
            value,
            json!({
                "code": "internal_error",
                "message": "internal server error"
            })
        );
        assert!(!encoded.contains("secret"));
        assert!(!encoded.contains("/private/path"));
        assert!(!encoded.contains("private source value"));
    }

    #[test]
    fn error_details_have_a_bounded_field_and_code_shape() {
        let response = ErrorResponse {
            code: "invalid_input".into(),
            message: "request contains invalid input".into(),
            details: Some(ErrorDetails {
                field_errors: vec![FieldError::from(JobSpecError::EmptyCommand)],
            }),
        };

        assert_eq!(
            serde_json::to_value(response).unwrap(),
            json!({
                "code": "invalid_input",
                "message": "request contains invalid input",
                "details": {
                    "field_errors": [{"field": "command", "code": "empty_command"}]
                }
            })
        );
        let details =
            serde_json::to_value(FieldError::from(JobSpecError::WorkingDirectoryTooLong {
                actual_bytes: 5000,
                max_bytes: 4096,
            }))
            .unwrap();
        assert_eq!(
            details,
            json!({"field": "working_dir", "code": "working_directory_too_long"})
        );
    }
}
