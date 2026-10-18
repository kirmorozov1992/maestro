use super::map_store_error;
use crate::{
    domain::{
        AgentId, Allocation, AllocationId, ApplyOutcome, Job, JobEvent, JobId, JobSnapshot,
        JobSpec, JobSpecError, JobStatus, StopReason, Timestamp,
    },
    error::AppError,
    state::{error::StateStoreError, store::StateStore},
};
use std::collections::BTreeMap;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct JobSubmission {
    pub(crate) command: String,
    pub(crate) args: Vec<String>,
    pub(crate) env: Option<BTreeMap<String, String>>,
    pub(crate) working_dir: Option<String>,
}

#[derive(Clone)]
pub(crate) struct JobsService {
    store: StateStore,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct StopRequest {
    pub(crate) job_id: JobId,
    pub(crate) allocation_id: AllocationId,
    pub(crate) agent_id: AgentId,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum StopOutcome {
    Completed(JobSnapshot),
    Requested(StopRequest),
}

#[cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "application services are connected to transport in M5"
    )
)]
impl JobsService {
    pub(crate) fn new(store: StateStore) -> Self {
        Self { store }
    }

    pub(crate) fn submit_job(
        &self,
        submission: JobSubmission,
        submitted_at: Timestamp,
    ) -> Result<JobSnapshot, AppError> {
        let spec = JobSpec::new(
            submission.command,
            submission.args,
            submission.env,
            submission.working_dir,
        )
        .map_err(map_job_spec_error)?;
        let job = Job::new(JobId::new(), spec, submitted_at);
        let snapshot = JobSnapshot::from(&job);
        self.store.create_job(job).map_err(map_store_error)?;
        Ok(snapshot)
    }

    pub(crate) fn get_job_status(&self, id: JobId) -> Result<JobSnapshot, AppError> {
        let job = self.store.get_job(id).map_err(map_store_error)?;
        Ok(JobSnapshot::from(&job))
    }

    pub(crate) fn list_jobs(&self) -> Result<Vec<JobSnapshot>, AppError> {
        self.store
            .list_jobs()
            .map(|jobs| jobs.iter().map(JobSnapshot::from).collect())
            .map_err(map_store_error)
    }

    pub(crate) fn report_event(&self, event: JobEvent) -> Result<ApplyOutcome, AppError> {
        self.store.append_event(&event).map_err(map_store_error)
    }

    pub(crate) fn stop_job(
        &self,
        id: JobId,
        stopped_at: Timestamp,
    ) -> Result<StopOutcome, AppError> {
        let job = self.store.get_job(id).map_err(map_store_error)?;
        match job.status() {
            JobStatus::Pending => {
                self.store
                    .append_event(&JobEvent::Stopped {
                        job_id: id,
                        allocation_id: None,
                        agent_id: None,
                        occurred_at: stopped_at,
                        reason: StopReason::UserRequested,
                    })
                    .map_err(map_store_error)?;
                let stopped_job = self.store.get_job(id).map_err(map_store_error)?;
                Ok(StopOutcome::Completed(JobSnapshot::from(&stopped_job)))
            }
            JobStatus::Assigned | JobStatus::Running => {
                let allocation = self
                    .store
                    .get_allocation_for_job(id)
                    .map_err(map_store_error)?;
                let request = stop_request_from_allocation(id, allocation)?;
                Ok(StopOutcome::Requested(request))
            }
            JobStatus::Succeeded | JobStatus::Failed | JobStatus::Stopped | JobStatus::Lost => Err(
                AppError::conflict(format!("job `{id}` is already terminal")),
            ),
        }
    }
}

fn map_job_spec_error(error: JobSpecError) -> AppError {
    AppError::invalid_input(format!("{}: {error}", error.code()))
}

fn stop_request_from_allocation(
    job_id: JobId,
    allocation: Option<Allocation>,
) -> Result<StopRequest, AppError> {
    let allocation = allocation.ok_or_else(|| {
        map_store_error(StateStoreError::InvariantViolation {
            context: "active job has no linked allocation",
        })
    })?;
    Ok(StopRequest {
        job_id,
        allocation_id: allocation.id(),
        agent_id: allocation.agent_id(),
    })
}

#[cfg(test)]
mod tests {
    use super::{JobSubmission, JobsService, StopOutcome};
    use crate::{
        domain::{
            Agent, AgentAvailability, AgentId, Allocation, AllocationId, AllocationStatus,
            ApplyOutcome, Job, JobEvent, JobId, JobSpec, JobStatus, MAX_ENVIRONMENT_BYTES,
            StopReason, Timestamp,
        },
        error::ErrorKind,
        state::{error::StateStoreError, store::StateStore},
    };
    use std::{collections::BTreeMap, num::NonZeroU32};

    fn id(value: u32) -> JobId {
        format!("00000000-0000-4000-8000-{value:012x}")
            .parse()
            .expect("fixed UUID is valid")
    }

    fn agent_id(value: u32) -> AgentId {
        format!("00000000-0000-4000-8001-{value:012x}")
            .parse()
            .expect("fixed UUID is valid")
    }

    fn allocation_id(value: u32) -> AllocationId {
        format!("00000000-0000-4000-8002-{value:012x}")
            .parse()
            .expect("fixed UUID is valid")
    }

    fn service() -> JobsService {
        JobsService::new(StateStore::default())
    }

    fn submission(command: &str) -> JobSubmission {
        JobSubmission {
            command: command.into(),
            args: vec!["hello".into()],
            env: None,
            working_dir: None,
        }
    }

    #[test]
    fn rejects_oversized_environment_without_mutating_store() {
        let service = service();
        let request = JobSubmission {
            command: "echo".into(),
            args: Vec::new(),
            env: Some(BTreeMap::from([(
                "KEY".into(),
                "x".repeat(MAX_ENVIRONMENT_BYTES),
            )])),
            working_dir: None,
        };

        let error = service
            .submit_job(request, Timestamp::from_unix_millis(100))
            .unwrap_err();

        assert_eq!(error.kind(), ErrorKind::InvalidInput);
        assert!(service.list_jobs().unwrap().is_empty());
    }

    #[test]
    fn rejects_invalid_job_submission_without_mutating_store() {
        let service = service();

        let error = service
            .submit_job(submission("  "), Timestamp::from_unix_millis(100))
            .unwrap_err();

        assert_eq!(error.kind(), ErrorKind::InvalidInput);
        assert!(service.list_jobs().unwrap().is_empty());
    }

    #[test]
    fn valid_submission_creates_pending_snapshot_at_supplied_timestamp() {
        let service = service();
        let submitted_at = Timestamp::from_unix_millis(1234);

        let snapshot = service
            .submit_job(submission("echo"), submitted_at)
            .unwrap();

        assert_eq!(snapshot.status, JobStatus::Pending);
        assert_eq!(snapshot.submitted_at, submitted_at);
        assert_eq!(snapshot.updated_at, submitted_at);
        assert_eq!(snapshot.spec.command(), "echo");
        assert_eq!(snapshot.spec.args(), &["hello"]);
        assert_eq!(service.get_job_status(snapshot.id).unwrap(), snapshot);
    }

    #[test]
    fn unknown_job_status_maps_to_not_found() {
        let error = service().get_job_status(id(99)).unwrap_err();

        assert_eq!(error.kind(), ErrorKind::NotFound);
    }

    #[test]
    fn list_snapshots_preserve_deterministic_store_order() {
        let service = service();
        for timestamp in [300, 100, 200] {
            service
                .submit_job(submission("echo"), Timestamp::from_unix_millis(timestamp))
                .unwrap();
        }

        let timestamps = service
            .list_jobs()
            .unwrap()
            .into_iter()
            .map(|snapshot| snapshot.submitted_at.as_unix_millis())
            .collect::<Vec<_>>();

        assert_eq!(timestamps, vec![100, 200, 300]);
    }

    #[test]
    fn report_event_returns_applied_duplicate_and_noop() {
        let service = service();
        let submitted_at = Timestamp::from_unix_millis(100);
        let snapshot = service
            .submit_job(submission("echo"), submitted_at)
            .unwrap();
        let stop = JobEvent::Stopped {
            job_id: snapshot.id,
            allocation_id: None,
            agent_id: None,
            occurred_at: Timestamp::from_unix_millis(200),
            reason: StopReason::UserRequested,
        };

        assert_eq!(
            service
                .report_event(JobEvent::Submitted {
                    job_id: snapshot.id,
                    occurred_at: submitted_at,
                })
                .unwrap(),
            ApplyOutcome::Noop
        );
        assert_eq!(
            service.report_event(stop.clone()).unwrap(),
            ApplyOutcome::Applied
        );
        assert_eq!(service.report_event(stop).unwrap(), ApplyOutcome::Duplicate);
    }

    #[test]
    fn report_event_maps_transition_rejection_to_conflict() {
        let service = service();
        let job_id = id(1);
        let allocation_id = allocation_id(1);
        let assigned_agent_id = agent_id(1);
        let wrong_agent_id = agent_id(2);
        let assigned_at = Timestamp::from_unix_millis(200);
        service
            .store
            .create_job(Job::new(
                job_id,
                JobSpec::new("echo", Vec::new(), None, None).unwrap(),
                Timestamp::from_unix_millis(100),
            ))
            .unwrap();
        service
            .store
            .register_agent(Agent::new(
                assigned_agent_id,
                "worker.example:9000".into(),
                Timestamp::from_unix_millis(100),
            ))
            .unwrap();
        service
            .store
            .reserve_agent(assigned_agent_id, allocation_id)
            .unwrap();
        service
            .store
            .create_allocation(Allocation::new(
                allocation_id,
                job_id,
                assigned_agent_id,
                NonZeroU32::new(1).unwrap(),
                assigned_at,
            ))
            .unwrap();
        service
            .report_event(JobEvent::Assigned {
                job_id,
                allocation_id,
                agent_id: assigned_agent_id,
                occurred_at: assigned_at,
            })
            .unwrap();
        let before_job = service.store.get_job(job_id).unwrap();
        let before_allocation = service.store.get_allocation(allocation_id).unwrap();

        let error = service
            .report_event(JobEvent::Started {
                job_id,
                allocation_id,
                agent_id: wrong_agent_id,
                occurred_at: Timestamp::from_unix_millis(300),
            })
            .unwrap_err();

        assert_eq!(error.kind(), ErrorKind::Conflict);
        assert!(error.to_string().contains("agent id mismatch"));
        assert_eq!(service.store.get_job(job_id).unwrap(), before_job);
        assert_eq!(
            service.store.get_allocation(allocation_id).unwrap(),
            before_allocation
        );
    }

    fn service_with_allocation(running: bool) -> (JobsService, JobId, AllocationId, AgentId) {
        let service = service();
        let job_id = id(10);
        let allocation_id = allocation_id(10);
        let assigned_agent_id = agent_id(10);
        let assigned_at = Timestamp::from_unix_millis(200);
        service
            .store
            .create_job(Job::new(
                job_id,
                JobSpec::new("echo", Vec::new(), None, None).unwrap(),
                Timestamp::from_unix_millis(100),
            ))
            .unwrap();
        service
            .store
            .register_agent(Agent::new(
                assigned_agent_id,
                "worker.example:9000".into(),
                Timestamp::from_unix_millis(100),
            ))
            .unwrap();
        service
            .store
            .reserve_agent(assigned_agent_id, allocation_id)
            .unwrap();
        service
            .store
            .create_allocation(Allocation::new(
                allocation_id,
                job_id,
                assigned_agent_id,
                NonZeroU32::new(1).unwrap(),
                assigned_at,
            ))
            .unwrap();
        service
            .report_event(JobEvent::Assigned {
                job_id,
                allocation_id,
                agent_id: assigned_agent_id,
                occurred_at: assigned_at,
            })
            .unwrap();
        if running {
            service
                .report_event(JobEvent::Started {
                    job_id,
                    allocation_id,
                    agent_id: assigned_agent_id,
                    occurred_at: Timestamp::from_unix_millis(300),
                })
                .unwrap();
        }
        (service, job_id, allocation_id, assigned_agent_id)
    }

    #[test]
    fn stop_pending_job_applies_stopped_event() {
        let service = service();
        let submitted = service
            .submit_job(submission("echo"), Timestamp::from_unix_millis(100))
            .unwrap();

        let outcome = service
            .stop_job(submitted.id, Timestamp::from_unix_millis(200))
            .unwrap();

        let StopOutcome::Completed(snapshot) = outcome else {
            panic!("pending job stop should complete synchronously");
        };
        assert_eq!(snapshot.status, JobStatus::Stopped);
        assert_eq!(snapshot.finished_at, Some(Timestamp::from_unix_millis(200)));
        assert_eq!(service.get_job_status(submitted.id).unwrap(), snapshot);
    }

    #[test]
    fn stop_pending_job_with_created_allocation_returns_conflict_without_mutation() {
        let service = service();
        let job_id = id(11);
        let allocation_id = allocation_id(11);
        let assigned_agent_id = agent_id(11);
        let assigned_at = Timestamp::from_unix_millis(200);
        service
            .store
            .create_job(Job::new(
                job_id,
                JobSpec::new("echo", Vec::new(), None, None).unwrap(),
                Timestamp::from_unix_millis(100),
            ))
            .unwrap();
        service
            .store
            .register_agent(Agent::new(
                assigned_agent_id,
                "worker.example:9000".into(),
                Timestamp::from_unix_millis(100),
            ))
            .unwrap();
        service
            .store
            .reserve_agent(assigned_agent_id, allocation_id)
            .unwrap();
        service
            .store
            .create_allocation(Allocation::new(
                allocation_id,
                job_id,
                assigned_agent_id,
                NonZeroU32::new(1).unwrap(),
                assigned_at,
            ))
            .unwrap();

        let error = service
            .stop_job(job_id, Timestamp::from_unix_millis(300))
            .unwrap_err();

        assert_eq!(error.kind(), ErrorKind::Conflict);
        assert_eq!(
            service.store.get_job(job_id).unwrap().status(),
            JobStatus::Pending
        );
        assert_eq!(
            service
                .store
                .get_allocation(allocation_id)
                .unwrap()
                .status(),
            AllocationStatus::Assigned
        );
        assert_eq!(
            service
                .store
                .get_agent(assigned_agent_id)
                .unwrap()
                .availability(),
            AgentAvailability::Busy
        );
    }

    #[test]
    fn stop_assigned_job_returns_request_without_terminal_mutation() {
        let (service, job_id, allocation_id, assigned_agent_id) = service_with_allocation(false);
        let before_job = service.store.get_job(job_id).unwrap();
        let before_allocation = service.store.get_allocation(allocation_id).unwrap();

        let outcome = service
            .stop_job(job_id, Timestamp::from_unix_millis(400))
            .unwrap();

        let StopOutcome::Requested(request) = outcome else {
            panic!("assigned job stop should return a request");
        };
        assert_eq!(request.job_id, job_id);
        assert_eq!(request.allocation_id, allocation_id);
        assert_eq!(request.agent_id, assigned_agent_id);
        assert_eq!(service.store.get_job(job_id).unwrap(), before_job);
        assert_eq!(
            service.store.get_allocation(allocation_id).unwrap(),
            before_allocation
        );
    }

    #[test]
    fn stop_running_job_returns_request_without_terminal_mutation() {
        let (service, job_id, allocation_id, assigned_agent_id) = service_with_allocation(true);
        let before_job = service.store.get_job(job_id).unwrap();
        let before_allocation = service.store.get_allocation(allocation_id).unwrap();

        let outcome = service
            .stop_job(job_id, Timestamp::from_unix_millis(400))
            .unwrap();

        let StopOutcome::Requested(request) = outcome else {
            panic!("running job stop should return a request");
        };
        assert_eq!(request.job_id, job_id);
        assert_eq!(request.allocation_id, allocation_id);
        assert_eq!(request.agent_id, assigned_agent_id);
        assert_eq!(service.store.get_job(job_id).unwrap(), before_job);
        assert_eq!(
            service.store.get_allocation(allocation_id).unwrap(),
            before_allocation
        );
    }

    #[test]
    fn stop_terminal_job_returns_conflict() {
        let service = service();
        let submitted = service
            .submit_job(submission("echo"), Timestamp::from_unix_millis(100))
            .unwrap();
        service
            .report_event(JobEvent::Stopped {
                job_id: submitted.id,
                allocation_id: None,
                agent_id: None,
                occurred_at: Timestamp::from_unix_millis(200),
                reason: StopReason::UserRequested,
            })
            .unwrap();

        let error = service
            .stop_job(submitted.id, Timestamp::from_unix_millis(300))
            .unwrap_err();

        assert_eq!(error.kind(), ErrorKind::Conflict);
    }

    #[test]
    fn active_stop_without_allocation_returns_internal_error() {
        let error = super::stop_request_from_allocation(id(20), None).unwrap_err();

        assert_eq!(error.kind(), ErrorKind::Internal);
        assert!(
            std::error::Error::source(&error)
                .and_then(|source| source.downcast_ref::<StateStoreError>())
                .is_some()
        );
    }
}
