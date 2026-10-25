use crate::{
    domain::{Agent, AgentAvailability, AgentHealth, AgentId, Allocation, AllocationId, Timestamp},
    state::{
        error::StateStoreError,
        store::{QueuedAssignment, StateStore},
    },
};
use std::{error::Error, fmt};
use std::{num::NonZeroU32, time::Duration};

pub(crate) struct Scheduler {
    store: StateStore,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TickOutcome {
    NoPendingJobs,
    NoEligibleAgent,
    Assigned(QueuedAssignment),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum SchedulerError {
    ZeroAssignmentTimeout,
    StateStore(StateStoreError),
}

impl From<StateStoreError> for SchedulerError {
    fn from(error: StateStoreError) -> Self {
        Self::StateStore(error)
    }
}

impl fmt::Display for SchedulerError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ZeroAssignmentTimeout => {
                formatter.write_str("assignment timeout must be greater than zero")
            }
            Self::StateStore(error) => error.fmt(formatter),
        }
    }
}

impl Error for SchedulerError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::StateStore(error) => Some(error),
            Self::ZeroAssignmentTimeout => None,
        }
    }
}

impl Scheduler {
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "M4.5 server loop will construct the scheduler")
    )]
    pub(crate) fn new(store: StateStore) -> Self {
        Self { store }
    }

    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "M4.5 server loop will invoke scheduler ticks")
    )]
    pub(crate) fn tick(
        &self,
        now: Timestamp,
        assignment_timeout: Duration,
    ) -> Result<TickOutcome, SchedulerError> {
        if assignment_timeout.is_zero() {
            return Err(SchedulerError::ZeroAssignmentTimeout);
        }
        self.store.expire_assignments(now, assignment_timeout)?;
        let Some(job) = self.store.list_pending_jobs()?.into_iter().next() else {
            return Ok(TickOutcome::NoPendingJobs);
        };
        let agents = self.store.list_agents()?;
        let Some(agent_id) = select_agent(&agents) else {
            return Ok(TickOutcome::NoEligibleAgent);
        };

        let allocation = Allocation::new(
            AllocationId::new(),
            job.id(),
            agent_id,
            NonZeroU32::MIN,
            now,
        );
        Ok(TickOutcome::Assigned(
            self.store.assign_allocation(allocation)?,
        ))
    }
}

pub(crate) fn select_agent(agents: &[Agent]) -> Option<AgentId> {
    agents
        .iter()
        .filter(|agent| {
            agent.health() == AgentHealth::Healthy
                && agent.availability() == AgentAvailability::Idle
        })
        .map(Agent::id)
        .min()
}

#[cfg(test)]
mod tests {
    use super::{Scheduler, TickOutcome, select_agent};
    use crate::{
        domain::{
            Agent, AgentAvailability, AgentId, Allocation, AllocationId, AllocationStatus, Job,
            JobId, JobSpec, JobStatus, Timestamp,
        },
        state::store::StateStore,
    };
    use std::{collections::BTreeMap, num::NonZeroU32, time::Duration};

    fn job_id(suffix: u32) -> JobId {
        format!("00000000-0000-4000-8000-{suffix:012x}")
            .parse()
            .expect("fixed UUID is valid")
    }

    fn agent_id(suffix: u32) -> AgentId {
        format!("00000000-0000-4000-8001-{suffix:012x}")
            .parse()
            .expect("fixed UUID is valid")
    }

    fn allocation_id(suffix: u32) -> AllocationId {
        format!("00000000-0000-4000-8002-{suffix:012x}")
            .parse()
            .expect("fixed UUID is valid")
    }

    fn agent(suffix: u32, registered_at: u64) -> Agent {
        Agent::new(
            agent_id(suffix),
            "worker.example:9000".into(),
            Timestamp::from_unix_millis(registered_at),
        )
    }

    fn job(id: JobId, submitted_at: u64) -> Job {
        Job::new(
            id,
            JobSpec::new(
                "echo",
                vec!["hello".into()],
                Some(BTreeMap::from([("MODE".into(), "test".into())])),
                None,
            )
            .expect("fixture job spec is valid"),
            Timestamp::from_unix_millis(submitted_at),
        )
    }

    fn id_tick(result: TickOutcome) -> (JobId, AgentId, AllocationId) {
        match result {
            TickOutcome::Assigned(assignment) => (
                assignment.job_id(),
                assignment.agent_id(),
                assignment.allocation_id(),
            ),
            other => panic!("expected assignment, got {other:?}"),
        }
    }

    #[test]
    fn select_agent_returns_none_for_empty_snapshot() {
        assert_eq!(select_agent(&[]), None);
    }

    #[test]
    fn select_agent_filters_candidates_and_is_independent_of_input_order() {
        let mut busy = agent(1, 100);
        busy.set_availability(AgentAvailability::Busy);
        let mut unhealthy = agent(2, 100);
        unhealthy.mark_unhealthy();
        let candidates = vec![agent(9, 100), busy, unhealthy, agent(4, 100)];

        assert_eq!(select_agent(&candidates), Some(agent_id(4)));
        assert_eq!(
            select_agent(&candidates.into_iter().rev().collect::<Vec<_>>()),
            Some(agent_id(4))
        );
    }

    #[test]
    fn select_agent_returns_none_when_no_candidate_is_healthy_and_idle() {
        let mut busy = agent(1, 100);
        busy.set_availability(AgentAvailability::Busy);
        let mut unhealthy = agent(2, 100);
        unhealthy.mark_unhealthy();
        assert_eq!(select_agent(&[busy, unhealthy]), None);
    }

    #[test]
    fn tick_assigns_fifo_job_to_lowest_healthy_idle_agent_and_queues_assignment() {
        let store = StateStore::default();
        let first_job_id = job_id(1);
        let second_job_id = job_id(2);
        store.create_job(job(first_job_id, 200)).unwrap();
        store.create_job(job(second_job_id, 100)).unwrap();
        store.register_agent(agent(9, 100)).unwrap();
        store.register_agent(agent(4, 100)).unwrap();
        let scheduler = Scheduler::new(store.clone());

        let result = scheduler
            .tick(Timestamp::from_unix_millis(300), Duration::from_secs(30))
            .unwrap();
        let (assigned_job_id, assigned_agent_id, allocation_id) = id_tick(result);

        assert_eq!(assigned_job_id, first_job_id);
        assert_eq!(assigned_agent_id, agent_id(4));
        assert_eq!(
            store.get_job(first_job_id).unwrap().status(),
            JobStatus::Assigned
        );
        assert_eq!(
            store.get_job(second_job_id).unwrap().status(),
            JobStatus::Pending
        );
        assert_eq!(
            store.get_agent(agent_id(4)).unwrap().availability(),
            AgentAvailability::Busy
        );
        assert_eq!(
            store.get_agent(agent_id(9)).unwrap().availability(),
            AgentAvailability::Idle
        );
        let allocation = store.get_allocation_for_job(first_job_id).unwrap().unwrap();
        assert_eq!(allocation.id(), allocation_id);
        assert_eq!(allocation.status(), AllocationStatus::Assigned);
        assert_eq!(allocation.attempt().get(), 1);
        assert_eq!(
            store
                .list_pending_jobs()
                .unwrap()
                .iter()
                .map(Job::id)
                .collect::<Vec<_>>(),
            vec![second_job_id]
        );
        assert_eq!(
            store.list_queued_assignments(agent_id(4)).unwrap(),
            vec![match result {
                TickOutcome::Assigned(assignment) => assignment,
                _ => unreachable!(),
            }]
        );
        assert_eq!(
            store.list_queued_assignments(agent_id(9)).unwrap(),
            Vec::new()
        );
    }

    #[test]
    fn tick_reports_no_pending_job_without_changing_agents() {
        let store = StateStore::default();
        store.register_agent(agent(1, 100)).unwrap();
        let scheduler = Scheduler::new(store.clone());

        assert_eq!(
            scheduler
                .tick(Timestamp::from_unix_millis(200), Duration::from_secs(30))
                .unwrap(),
            TickOutcome::NoPendingJobs
        );
        assert_eq!(
            store.get_agent(agent_id(1)).unwrap().availability(),
            AgentAvailability::Idle
        );
    }

    #[test]
    fn tick_keeps_job_pending_when_there_is_no_eligible_agent() {
        let store = StateStore::default();
        let pending_id = job_id(1);
        store.create_job(job(pending_id, 100)).unwrap();
        let mut busy = agent(1, 100);
        busy.set_availability(AgentAvailability::Busy);
        store.register_agent(busy).unwrap();
        let scheduler = Scheduler::new(store.clone());

        assert_eq!(
            scheduler
                .tick(Timestamp::from_unix_millis(200), Duration::from_secs(30))
                .unwrap(),
            TickOutcome::NoEligibleAgent
        );
        assert_eq!(
            store.get_job(pending_id).unwrap().status(),
            JobStatus::Pending
        );
        assert_eq!(store.list_pending_jobs().unwrap().len(), 1);
        assert_eq!(
            store.get_agent(agent_id(1)).unwrap().availability(),
            AgentAvailability::Busy
        );
    }

    #[test]
    fn tick_expires_assignment_before_scheduling_pending_job() {
        let store = StateStore::default();
        let expired_job_id = job_id(1);
        let pending_job_id = job_id(2);
        let agent_id = agent_id(1);
        store.create_job(job(expired_job_id, 100)).unwrap();
        store.create_job(job(pending_job_id, 200)).unwrap();
        store.register_agent(agent(1, 100)).unwrap();
        store
            .assign_allocation(Allocation::new(
                allocation_id(1),
                expired_job_id,
                agent_id,
                NonZeroU32::MIN,
                Timestamp::from_unix_millis(150),
            ))
            .unwrap();
        let scheduler = Scheduler::new(store.clone());

        let result = scheduler
            .tick(Timestamp::from_unix_millis(250), Duration::from_millis(100))
            .unwrap();
        let (assigned_job_id, assigned_agent_id, _) = id_tick(result);

        assert_eq!(assigned_job_id, pending_job_id);
        assert_eq!(assigned_agent_id, agent_id);
        assert_eq!(
            store.get_job(expired_job_id).unwrap().status(),
            JobStatus::Lost
        );
        assert_eq!(
            store.get_job(pending_job_id).unwrap().status(),
            JobStatus::Assigned
        );
        assert_eq!(
            store.get_agent(agent_id).unwrap().availability(),
            AgentAvailability::Busy
        );
    }

    #[test]
    fn tick_rejects_zero_assignment_timeout_without_expiring_assignments() {
        let store = StateStore::default();
        let assigned_job_id = job_id(1);
        let agent_id = agent_id(1);
        store.create_job(job(assigned_job_id, 100)).unwrap();
        store.register_agent(agent(1, 100)).unwrap();
        store
            .assign_allocation(Allocation::new(
                allocation_id(1),
                assigned_job_id,
                agent_id,
                NonZeroU32::MIN,
                Timestamp::from_unix_millis(150),
            ))
            .unwrap();
        let scheduler = Scheduler::new(store.clone());

        assert!(
            scheduler
                .tick(Timestamp::from_unix_millis(150), Duration::ZERO)
                .is_err()
        );
        assert_eq!(
            store.get_job(assigned_job_id).unwrap().status(),
            JobStatus::Assigned
        );
        assert_eq!(
            store.get_agent(agent_id).unwrap().availability(),
            AgentAvailability::Busy
        );
        assert_eq!(store.list_queued_assignments(agent_id).unwrap().len(), 1);
    }

    #[test]
    fn tick_error_does_not_partially_reserve_or_dequeue() {
        let store = StateStore::default();
        let pending_id = job_id(1);
        store.create_job(job(pending_id, 200)).unwrap();
        store.register_agent(agent(1, 100)).unwrap();
        let scheduler = Scheduler::new(store.clone());

        assert!(
            scheduler
                .tick(Timestamp::from_unix_millis(199), Duration::from_secs(30))
                .is_err()
        );
        assert_eq!(
            store.get_job(pending_id).unwrap().status(),
            JobStatus::Pending
        );
        assert_eq!(store.list_pending_jobs().unwrap().len(), 1);
        assert_eq!(
            store.get_agent(agent_id(1)).unwrap().availability(),
            AgentAvailability::Idle
        );
        assert!(store.get_allocation_for_job(pending_id).unwrap().is_none());
        assert!(
            store
                .list_queued_assignments(agent_id(1))
                .unwrap()
                .is_empty()
        );
    }
}
