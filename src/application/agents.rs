use super::map_store_error;
use crate::{
    domain::{
        Agent, AgentId, AgentSnapshot, AllocationId, AllocationSnapshot, JobSnapshot, Timestamp,
    },
    error::AppError,
    state::store::{AssignmentStartOutcome, StateStore},
};
use std::time::Duration;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct AgentAssignment {
    pub(crate) job: JobSnapshot,
    pub(crate) allocation: AllocationSnapshot,
}

#[derive(Clone)]
pub(crate) struct AgentsService {
    store: StateStore,
}

#[cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "application services are connected to transport in M5"
    )
)]
impl AgentsService {
    pub(crate) fn new(store: StateStore) -> Self {
        Self { store }
    }

    pub(crate) fn register_agent(
        &self,
        id: AgentId,
        address: String,
        registered_at: Timestamp,
    ) -> Result<AgentSnapshot, AppError> {
        if address.trim().is_empty() {
            return Err(AppError::invalid_input("agent address cannot be empty"));
        }

        let agent = Agent::new(id, address, registered_at);
        let snapshot = AgentSnapshot::from(&agent);
        self.store.register_agent(agent).map_err(map_store_error)?;
        Ok(snapshot)
    }

    pub(crate) fn heartbeat(
        &self,
        id: AgentId,
        timestamp: Timestamp,
    ) -> Result<AgentSnapshot, AppError> {
        self.store
            .update_heartbeat(id, timestamp)
            .map_err(map_store_error)?;
        let agent = self.store.get_agent(id).map_err(map_store_error)?;
        Ok(AgentSnapshot::from(&agent))
    }

    pub(crate) fn poll_assignment(
        &self,
        agent_id: AgentId,
        now: Timestamp,
        timeout: Duration,
    ) -> Result<Option<AgentAssignment>, AppError> {
        if timeout.is_zero() {
            return Err(AppError::invalid_input(
                "assignment timeout must be greater than zero",
            ));
        }

        self.store
            .poll_assignment(agent_id, now, timeout)
            .map(|assignment| {
                assignment.map(|(job, allocation)| AgentAssignment {
                    job: JobSnapshot::from(&job),
                    allocation: AllocationSnapshot::from(&allocation),
                })
            })
            .map_err(map_store_error)
    }

    pub(crate) fn acknowledge_start(
        &self,
        agent_id: AgentId,
        allocation_id: AllocationId,
        now: Timestamp,
        timeout: Duration,
    ) -> Result<AssignmentStartOutcome, AppError> {
        if timeout.is_zero() {
            return Err(AppError::invalid_input(
                "assignment timeout must be greater than zero",
            ));
        }

        self.store
            .acknowledge_start(agent_id, allocation_id, now, timeout)
            .map_err(map_store_error)
    }

    pub(crate) fn sweep_liveness(
        &self,
        now: Timestamp,
        timeout: Duration,
    ) -> Result<Vec<AgentId>, AppError> {
        if timeout.is_zero() {
            return Err(AppError::invalid_input(
                "liveness timeout must be greater than zero",
            ));
        }

        self.store
            .sweep_liveness(now, timeout)
            .map_err(map_store_error)
    }
}

#[cfg(test)]
mod tests {
    use super::AgentsService;
    use crate::{
        domain::{
            Agent, AgentAvailability, AgentHealth, AgentId, Allocation, AllocationId,
            AllocationStatus, Job, JobEvent, JobId, JobSpec, JobStatus, ProcessResult, Timestamp,
        },
        error::ErrorKind,
        state::{error::StateStoreError, store::StateStore},
    };
    use std::{
        num::NonZeroU32,
        sync::{Arc, Barrier},
        thread,
        time::Duration,
    };

    fn agent_id(value: u32) -> AgentId {
        format!("00000000-0000-4000-8001-{value:012x}")
            .parse()
            .expect("fixed UUID is valid")
    }

    fn service() -> AgentsService {
        AgentsService::new(StateStore::default())
    }

    fn job_id(value: u32) -> JobId {
        format!("00000000-0000-4000-8000-{value:012x}")
            .parse()
            .expect("fixed UUID is valid")
    }

    fn allocation_id(value: u32) -> AllocationId {
        format!("00000000-0000-4000-8002-{value:012x}")
            .parse()
            .expect("fixed UUID is valid")
    }

    fn scheduled_assignment(value: u32) -> (AgentsService, AgentId, JobId, AllocationId) {
        let service = service();
        let agent_id = agent_id(value);
        let job_id = job_id(value);
        let allocation_id = allocation_id(value);
        let assigned_at = Timestamp::from_unix_millis(200);
        service
            .register_agent(
                agent_id,
                "worker.example:9000".into(),
                Timestamp::from_unix_millis(100),
            )
            .unwrap();
        let job = Job::new(
            job_id,
            JobSpec::new("echo", vec!["hello".into()], None, None).unwrap(),
            Timestamp::from_unix_millis(100),
        );
        service.store.create_job(job).unwrap();
        service
            .store
            .assign_allocation(Allocation::new(
                allocation_id,
                job_id,
                agent_id,
                NonZeroU32::new(1).unwrap(),
                assigned_at,
            ))
            .unwrap();
        (service, agent_id, job_id, allocation_id)
    }

    #[test]
    fn register_agent_returns_healthy_idle_snapshot() {
        let service = service();
        let id = agent_id(1);
        let registered_at = Timestamp::from_unix_millis(100);

        let snapshot = service
            .register_agent(id, "worker.example:9000".into(), registered_at)
            .unwrap();

        assert_eq!(snapshot.id, id);
        assert_eq!(snapshot.address, "worker.example:9000");
        assert_eq!(snapshot.last_heartbeat_at, registered_at);
        assert_eq!(snapshot.health, AgentHealth::Healthy);
        assert_eq!(snapshot.availability, AgentAvailability::Idle);
    }

    #[test]
    fn register_agent_duplicate_id_returns_conflict_without_replacement() {
        let service = service();
        let id = agent_id(1);
        service
            .register_agent(
                id,
                "first.example:9000".into(),
                Timestamp::from_unix_millis(100),
            )
            .unwrap();
        let original = Agent::new(
            id,
            "first.example:9000".into(),
            Timestamp::from_unix_millis(100),
        );

        let error = service
            .register_agent(
                id,
                "replacement.example:9000".into(),
                Timestamp::from_unix_millis(200),
            )
            .unwrap_err();

        assert_eq!(error.kind(), ErrorKind::Conflict);
        assert_eq!(service.store.get_agent(id).unwrap(), original);
    }

    #[test]
    fn register_agent_rejects_blank_address_without_mutating_store() {
        for (suffix, address) in [(2, ""), (3, " \t\n")] {
            let service = service();
            let id = agent_id(suffix);

            let error = service
                .register_agent(id, address.into(), Timestamp::from_unix_millis(100))
                .unwrap_err();

            assert_eq!(error.kind(), ErrorKind::InvalidInput);
            assert!(matches!(
                service.store.get_agent(id),
                Err(StateStoreError::NotFound { .. })
            ));
        }
    }

    #[test]
    fn heartbeat_updates_known_agent_snapshot() {
        let service = service();
        let id = agent_id(1);
        service
            .register_agent(
                id,
                "worker.example:9000".into(),
                Timestamp::from_unix_millis(100),
            )
            .unwrap();

        let snapshot = service
            .heartbeat(id, Timestamp::from_unix_millis(200))
            .unwrap();

        assert_eq!(snapshot.id, id);
        assert_eq!(snapshot.last_heartbeat_at, Timestamp::from_unix_millis(200));
    }

    #[test]
    fn heartbeat_unknown_agent_returns_not_found() {
        let error = service()
            .heartbeat(agent_id(99), Timestamp::from_unix_millis(200))
            .unwrap_err();

        assert_eq!(error.kind(), ErrorKind::NotFound);
    }

    #[test]
    fn heartbeat_stale_timestamp_returns_conflict_without_rewinding() {
        let service = service();
        let id = agent_id(1);
        service
            .register_agent(
                id,
                "worker.example:9000".into(),
                Timestamp::from_unix_millis(100),
            )
            .unwrap();
        service
            .heartbeat(id, Timestamp::from_unix_millis(200))
            .unwrap();

        let error = service
            .heartbeat(id, Timestamp::from_unix_millis(199))
            .unwrap_err();

        assert_eq!(error.kind(), ErrorKind::Conflict);
        assert_eq!(
            service.store.get_agent(id).unwrap().last_heartbeat_at(),
            Timestamp::from_unix_millis(200)
        );
    }

    #[test]
    fn heartbeat_equal_timestamp_is_accepted() {
        let service = service();
        let id = agent_id(1);
        service
            .register_agent(
                id,
                "worker.example:9000".into(),
                Timestamp::from_unix_millis(100),
            )
            .unwrap();

        let snapshot = service
            .heartbeat(id, Timestamp::from_unix_millis(100))
            .unwrap();

        assert_eq!(snapshot.last_heartbeat_at, Timestamp::from_unix_millis(100));
    }

    #[test]
    fn concurrent_equal_heartbeats_are_both_accepted() {
        let service = service();
        let id = agent_id(1);
        service
            .register_agent(
                id,
                "worker.example:9000".into(),
                Timestamp::from_unix_millis(100),
            )
            .unwrap();
        let timestamp = Timestamp::from_unix_millis(200);
        let barrier = Arc::new(Barrier::new(3));
        let handles = [service.clone(), service.clone()].map(|service| {
            let barrier = Arc::clone(&barrier);
            thread::spawn(move || {
                barrier.wait();
                service.heartbeat(id, timestamp)
            })
        });

        barrier.wait();
        let results = handles.map(|handle| handle.join().unwrap());

        assert!(results.iter().all(Result::is_ok));
        assert!(
            results
                .iter()
                .all(|result| result.as_ref().unwrap().last_heartbeat_at == timestamp)
        );
        assert_eq!(
            service.store.get_agent(id).unwrap().last_heartbeat_at(),
            timestamp
        );
    }

    #[test]
    fn liveness_sweep_returns_newly_unhealthy_agent_ids() {
        let service = service();
        let id = agent_id(1);
        service
            .register_agent(
                id,
                "worker.example:9000".into(),
                Timestamp::from_unix_millis(100),
            )
            .unwrap();

        let unhealthy = service
            .sweep_liveness(Timestamp::from_unix_millis(200), Duration::from_millis(100))
            .unwrap();

        assert_eq!(unhealthy, vec![id]);
        assert_eq!(
            service.store.get_agent(id).unwrap().health(),
            AgentHealth::Unhealthy
        );
        let late_heartbeat = service
            .heartbeat(id, Timestamp::from_unix_millis(250))
            .unwrap();
        assert_eq!(late_heartbeat.health, AgentHealth::Unhealthy);
        assert!(
            service
                .sweep_liveness(Timestamp::from_unix_millis(300), Duration::from_millis(100),)
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn liveness_sweep_rejects_zero_timeout_without_mutating_store() {
        let service = service();
        let id = agent_id(1);
        service
            .register_agent(
                id,
                "worker.example:9000".into(),
                Timestamp::from_unix_millis(100),
            )
            .unwrap();

        let error = service
            .sweep_liveness(Timestamp::from_unix_millis(200), Duration::ZERO)
            .unwrap_err();

        assert_eq!(error.kind(), ErrorKind::InvalidInput);
        assert_eq!(
            service.store.get_agent(id).unwrap().health(),
            AgentHealth::Healthy
        );
    }

    #[test]
    fn repeated_assignment_polls_return_the_same_owned_assignment_until_started() {
        let (service, agent_id, _, allocation_id) = scheduled_assignment(21);
        let now = Timestamp::from_unix_millis(250);
        let timeout = Duration::from_millis(1_000);

        let first = service
            .poll_assignment(agent_id, now, timeout)
            .unwrap()
            .unwrap();
        let second = service
            .poll_assignment(agent_id, now, timeout)
            .unwrap()
            .unwrap();

        assert_eq!(first, second);
        assert_eq!(first.allocation.id, allocation_id);
        assert_eq!(first.allocation.status, AllocationStatus::Assigned);
        assert_eq!(
            service
                .store
                .list_queued_assignments(agent_id)
                .unwrap()
                .len(),
            1
        );
    }

    #[test]
    fn poll_at_assignment_deadline_marks_lost_and_releases_reservation() {
        let (service, agent_id, job_id, allocation_id) = scheduled_assignment(22);
        let deadline = Timestamp::from_unix_millis(1_200);

        let assignment = service
            .poll_assignment(agent_id, deadline, Duration::from_millis(1_000))
            .unwrap();

        assert!(assignment.is_none());
        assert_eq!(
            service.store.get_job(job_id).unwrap().status(),
            JobStatus::Lost
        );
        let allocation = service.store.get_allocation(allocation_id).unwrap();
        assert_eq!(allocation.status(), AllocationStatus::Lost);
        assert_eq!(
            service.store.get_agent(agent_id).unwrap().availability(),
            AgentAvailability::Idle
        );
        assert!(
            service
                .store
                .list_queued_assignments(agent_id)
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn zero_assignment_timeout_is_rejected_without_mutating_store() {
        let (service, agent_id, job_id, allocation_id) = scheduled_assignment(23);

        let now = Timestamp::from_unix_millis(250);
        let error = service
            .poll_assignment(agent_id, now, Duration::ZERO)
            .unwrap_err();
        let start_error = service
            .acknowledge_start(agent_id, allocation_id, now, Duration::ZERO)
            .unwrap_err();

        assert_eq!(error.kind(), ErrorKind::InvalidInput);
        assert_eq!(start_error.kind(), ErrorKind::InvalidInput);
        assert_eq!(
            service.store.get_job(job_id).unwrap().status(),
            JobStatus::Assigned
        );
        assert_eq!(
            service
                .store
                .get_allocation(allocation_id)
                .unwrap()
                .status(),
            AllocationStatus::Assigned
        );
    }

    #[test]
    fn only_assigned_agent_can_acknowledge_start_and_retries_are_idempotent() {
        let (service, assigned_agent_id, job_id, allocation_id) = scheduled_assignment(24);
        let timestamp = Timestamp::from_unix_millis(250);
        let timeout = Duration::from_millis(1_000);
        service
            .register_agent(
                agent_id(25),
                "other-worker.example:9000".into(),
                Timestamp::from_unix_millis(100),
            )
            .unwrap();

        let wrong_agent = service
            .acknowledge_start(agent_id(25), allocation_id, timestamp, timeout)
            .unwrap_err();
        assert_eq!(wrong_agent.kind(), ErrorKind::Conflict);
        assert_eq!(
            service.store.get_job(job_id).unwrap().status(),
            JobStatus::Assigned
        );

        let first = service
            .acknowledge_start(assigned_agent_id, allocation_id, timestamp, timeout)
            .unwrap();
        let retry = service
            .acknowledge_start(assigned_agent_id, allocation_id, timestamp, timeout)
            .unwrap();

        assert_eq!(first, super::AssignmentStartOutcome::Started);
        assert_eq!(retry, super::AssignmentStartOutcome::Duplicate);
        assert_eq!(
            service.store.get_job(job_id).unwrap().status(),
            JobStatus::Running
        );
        assert!(
            service
                .store
                .list_queued_assignments(assigned_agent_id)
                .unwrap()
                .is_empty()
        );
        assert!(
            service
                .poll_assignment(assigned_agent_id, timestamp, timeout)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn start_at_assignment_deadline_expires_instead_of_starting() {
        let (service, agent_id, job_id, allocation_id) = scheduled_assignment(26);

        let outcome = service
            .acknowledge_start(
                agent_id,
                allocation_id,
                Timestamp::from_unix_millis(1_200),
                Duration::from_millis(1_000),
            )
            .unwrap();

        assert_eq!(outcome, super::AssignmentStartOutcome::Expired);
        assert_eq!(
            service.store.get_job(job_id).unwrap().status(),
            JobStatus::Lost
        );
        assert_eq!(
            service
                .store
                .get_allocation(allocation_id)
                .unwrap()
                .status(),
            AllocationStatus::Lost
        );
        assert_eq!(
            service.store.get_agent(agent_id).unwrap().availability(),
            AgentAvailability::Idle
        );
    }

    #[test]
    fn stale_start_for_previous_allocation_does_not_change_new_reservation() {
        let (service, agent_id, old_job_id, old_allocation_id) = scheduled_assignment(27);
        service
            .store
            .append_event(&JobEvent::Finished {
                job_id: old_job_id,
                allocation_id: old_allocation_id,
                agent_id,
                occurred_at: Timestamp::from_unix_millis(220),
                result: ProcessResult::SpawnFailed,
            })
            .unwrap();
        let new_job_id = job_id(28);
        let new_allocation_id = allocation_id(28);
        service
            .store
            .create_job(Job::new(
                new_job_id,
                JobSpec::new("echo", vec!["again".into()], None, None).unwrap(),
                Timestamp::from_unix_millis(230),
            ))
            .unwrap();
        service
            .store
            .assign_allocation(Allocation::new(
                new_allocation_id,
                new_job_id,
                agent_id,
                NonZeroU32::new(1).unwrap(),
                Timestamp::from_unix_millis(240),
            ))
            .unwrap();

        let error = service
            .acknowledge_start(
                agent_id,
                old_allocation_id,
                Timestamp::from_unix_millis(250),
                Duration::from_millis(1_000),
            )
            .unwrap_err();

        assert_eq!(error.kind(), ErrorKind::Conflict);
        assert_eq!(
            service.store.get_job(new_job_id).unwrap().status(),
            JobStatus::Assigned
        );
        assert_eq!(
            service
                .store
                .get_allocation(new_allocation_id)
                .unwrap()
                .status(),
            AllocationStatus::Assigned
        );
        assert_eq!(
            service
                .store
                .list_queued_assignments(agent_id)
                .unwrap()
                .len(),
            1
        );
    }
}
