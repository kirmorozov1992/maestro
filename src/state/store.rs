use super::error::StateStoreError;
use crate::domain::{
    Agent, AgentAvailability, AgentHealth, AgentId, Allocation, AllocationId, AllocationStatus,
    ApplyOutcome, Job, JobEvent, JobId, JobStatus, Timestamp, apply_event,
};
use std::{
    collections::{HashMap, VecDeque},
    sync::{Arc, Mutex, MutexGuard},
    time::Duration,
};

// Application services use this crate-private boundary for synchronous state
// operations.
#[derive(Clone, Default)]
pub(crate) struct StateStore {
    inner: Arc<Mutex<StoreState>>,
}

// These collections hold the state coordinated by StateStore operations.
#[derive(Default)]
struct StoreState {
    jobs: HashMap<JobId, Job>,
    agents: HashMap<AgentId, Agent>,
    allocations: HashMap<AllocationId, Allocation>,
    pending_jobs: VecDeque<JobId>,
    agent_allocations: HashMap<AgentId, AllocationId>,
}

impl StateStore {
    fn lock(&self) -> Result<MutexGuard<'_, StoreState>, StateStoreError> {
        self.inner.lock().map_err(|_| StateStoreError::LockPoisoned)
    }

    pub(crate) fn create_job(&self, job: Job) -> Result<(), StateStoreError> {
        let id = job.id();
        if job.status() != JobStatus::Pending {
            return Err(StateStoreError::conflict(
                "job",
                id,
                "a newly created job must be pending",
            ));
        }

        let mut state = self.lock()?;
        if state.jobs.contains_key(&id) {
            return Err(StateStoreError::already_exists("job", id));
        }
        if state.pending_jobs.contains(&id) {
            return Err(StateStoreError::invariant(
                "pending queue contains a job absent from the job map",
            ));
        }

        state.jobs.insert(id, job);
        state.pending_jobs.push_back(id);
        Ok(())
    }

    pub(crate) fn get_job(&self, id: JobId) -> Result<Job, StateStoreError> {
        let state = self.lock()?;
        state
            .jobs
            .get(&id)
            .cloned()
            .ok_or_else(|| StateStoreError::not_found("job", id))
    }

    pub(crate) fn list_jobs(&self) -> Result<Vec<Job>, StateStoreError> {
        let mut jobs = {
            let state = self.lock()?;
            state.jobs.values().cloned().collect::<Vec<_>>()
        };
        jobs.sort_by_key(|job| (job.submitted_at(), job.id()));
        Ok(jobs)
    }

    pub(crate) fn register_agent(&self, agent: Agent) -> Result<(), StateStoreError> {
        let id = agent.id();
        let mut state = self.lock()?;
        if state.agents.contains_key(&id) {
            return Err(StateStoreError::already_exists("agent", id));
        }
        if state.agent_allocations.contains_key(&id) {
            return Err(StateStoreError::invariant(
                "agent reservation exists without a registered agent",
            ));
        }

        state.agents.insert(id, agent);
        Ok(())
    }

    pub(crate) fn get_agent(&self, id: AgentId) -> Result<Agent, StateStoreError> {
        let state = self.lock()?;
        state
            .agents
            .get(&id)
            .cloned()
            .ok_or_else(|| StateStoreError::not_found("agent", id))
    }

    pub(crate) fn update_heartbeat(
        &self,
        id: AgentId,
        timestamp: Timestamp,
    ) -> Result<(), StateStoreError> {
        let mut state = self.lock()?;
        let agent = state
            .agents
            .get_mut(&id)
            .ok_or_else(|| StateStoreError::not_found("agent", id))?;
        let recorded_at = agent.last_heartbeat_at();
        if timestamp < recorded_at {
            return Err(StateStoreError::conflict(
                "agent",
                id,
                format!(
                    "heartbeat timestamp {} is older than recorded timestamp {}",
                    timestamp.as_unix_millis(),
                    recorded_at.as_unix_millis()
                ),
            ));
        }

        agent.update_last_heartbeat_at(timestamp);
        Ok(())
    }

    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "M4 scheduler orchestration will reserve available agents"
        )
    )]
    pub(crate) fn reserve_agent(
        &self,
        id: AgentId,
        allocation_id: AllocationId,
    ) -> Result<(), StateStoreError> {
        let mut state = self.lock()?;
        let mut agent = state
            .agents
            .get(&id)
            .cloned()
            .ok_or_else(|| StateStoreError::not_found("agent", id))?;

        if agent.health() != AgentHealth::Healthy {
            return Err(StateStoreError::conflict(
                "agent",
                id,
                "unhealthy agents cannot be reserved",
            ));
        }
        match (state.agent_allocations.get(&id), agent.availability()) {
            (Some(_), AgentAvailability::Busy) => {
                return Err(StateStoreError::conflict(
                    "agent",
                    id,
                    "agent already has a reservation",
                ));
            }
            (None, AgentAvailability::Idle) => {}
            _ => {
                return Err(StateStoreError::invariant(
                    "agent availability and reservation map disagree",
                ));
            }
        }
        if state.allocations.contains_key(&allocation_id)
            || state
                .agent_allocations
                .values()
                .any(|reserved_id| *reserved_id == allocation_id)
        {
            return Err(StateStoreError::already_exists(
                "allocation reservation",
                allocation_id,
            ));
        }

        agent.set_availability(AgentAvailability::Busy);
        state.agent_allocations.insert(id, allocation_id);
        state.agents.insert(id, agent);
        Ok(())
    }

    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "M4 scheduler orchestration will release unused reservations"
        )
    )]
    pub(crate) fn release_agent(
        &self,
        id: AgentId,
        allocation_id: AllocationId,
    ) -> Result<(), StateStoreError> {
        let mut state = self.lock()?;
        let mut agent = state
            .agents
            .get(&id)
            .cloned()
            .ok_or_else(|| StateStoreError::not_found("agent", id))?;
        let Some(current_allocation_id) = state.agent_allocations.get(&id).copied() else {
            return if agent.availability() == AgentAvailability::Idle {
                Err(StateStoreError::conflict(
                    "agent",
                    id,
                    "agent has no reservation to release",
                ))
            } else {
                Err(StateStoreError::invariant(
                    "busy agent has no reservation map entry",
                ))
            };
        };
        if current_allocation_id != allocation_id {
            return Err(StateStoreError::conflict(
                "agent",
                id,
                format!(
                    "reservation belongs to allocation {current_allocation_id}, not {allocation_id}"
                ),
            ));
        }
        if agent.availability() != AgentAvailability::Busy {
            return Err(StateStoreError::invariant(
                "reserved agent is not marked busy",
            ));
        }
        if state
            .allocations
            .get(&allocation_id)
            .is_some_and(|allocation| !is_terminal_allocation(allocation.status()))
        {
            return Err(StateStoreError::conflict(
                "allocation",
                allocation_id,
                "an active allocation must reach a terminal state before release",
            ));
        }

        agent.set_availability(AgentAvailability::Idle);
        state.agent_allocations.remove(&id);
        state.agents.insert(id, agent);
        Ok(())
    }

    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "M4 scheduler orchestration will create allocations"
        )
    )]
    pub(crate) fn create_allocation(&self, allocation: Allocation) -> Result<(), StateStoreError> {
        let id = allocation.id();
        let job_id = allocation.job_id();
        let agent_id = allocation.agent_id();
        let mut state = self.lock()?;
        if state.allocations.contains_key(&id) {
            return Err(StateStoreError::already_exists("allocation", id));
        }
        if allocation.status() != AllocationStatus::Assigned {
            return Err(StateStoreError::conflict(
                "allocation",
                id,
                "a newly created allocation must be assigned",
            ));
        }

        let job = state
            .jobs
            .get(&job_id)
            .ok_or_else(|| StateStoreError::not_found("job", job_id))?;
        if job.status() != JobStatus::Pending {
            return Err(StateStoreError::conflict(
                "job",
                job_id,
                format!("allocation requires a pending job, got {:?}", job.status()),
            ));
        }
        if pending_job_count(&state, job_id) != 1 {
            return Err(StateStoreError::invariant(
                "pending job must appear exactly once in the pending queue",
            ));
        }

        let agent = state
            .agents
            .get(&agent_id)
            .ok_or_else(|| StateStoreError::not_found("agent", agent_id))?;
        if agent.health() != AgentHealth::Healthy
            || agent.availability() != AgentAvailability::Busy
            || state.agent_allocations.get(&agent_id) != Some(&id)
        {
            return Err(StateStoreError::conflict(
                "agent",
                agent_id,
                "allocation requires a healthy agent reserved for its allocation ID",
            ));
        }
        if let Some(existing) = state
            .allocations
            .values()
            .find(|existing| existing.job_id() == job_id)
        {
            return Err(StateStoreError::conflict(
                "job",
                job_id,
                format!("allocation {} already exists for this job", existing.id()),
            ));
        }

        state.allocations.insert(id, allocation);
        Ok(())
    }

    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "M4 scheduler orchestration will inspect allocations"
        )
    )]
    pub(crate) fn get_allocation(&self, id: AllocationId) -> Result<Allocation, StateStoreError> {
        let state = self.lock()?;
        state
            .allocations
            .get(&id)
            .cloned()
            .ok_or_else(|| StateStoreError::not_found("allocation", id))
    }

    pub(crate) fn get_allocation_for_job(
        &self,
        job_id: JobId,
    ) -> Result<Option<Allocation>, StateStoreError> {
        let allocation = {
            let state = self.lock()?;
            if !state.jobs.contains_key(&job_id) {
                return Err(StateStoreError::not_found("job", job_id));
            }
            state
                .allocations
                .values()
                .find(|allocation| allocation.job_id() == job_id)
                .cloned()
        };
        Ok(allocation)
    }

    pub(crate) fn append_event(&self, event: &JobEvent) -> Result<ApplyOutcome, StateStoreError> {
        let mut state = self.lock()?;
        apply_events_in_state(&mut state, std::slice::from_ref(event))
    }

    pub(crate) fn sweep_liveness(
        &self,
        now: Timestamp,
        timeout: Duration,
    ) -> Result<Vec<AgentId>, StateStoreError> {
        let candidates = {
            let state = self.lock()?;
            state
                .agents
                .values()
                .filter(|agent| {
                    agent.health() == AgentHealth::Healthy
                        && heartbeat_expired(now, agent.last_heartbeat_at(), timeout)
                })
                .map(Agent::id)
                .collect::<Vec<_>>()
        };

        let mut unhealthy_agents = Vec::with_capacity(candidates.len());
        for agent_id in candidates {
            let mut state = self.lock()?;
            let agent = state
                .agents
                .get(&agent_id)
                .cloned()
                .ok_or(StateStoreError::invariant(
                    "liveness candidate is no longer registered",
                ))?;
            if agent.health() != AgentHealth::Healthy
                || !heartbeat_expired(now, agent.last_heartbeat_at(), timeout)
            {
                continue;
            }

            if let Some(allocation_id) = state.agent_allocations.get(&agent_id).copied() {
                let allocation =
                    state
                        .allocations
                        .get(&allocation_id)
                        .ok_or(StateStoreError::invariant(
                            "agent reservation references a missing allocation",
                        ))?;
                if allocation.agent_id() != agent_id {
                    return Err(StateStoreError::invariant(
                        "agent reservation references another agent's allocation",
                    ));
                }
                let job_id = allocation.job_id();
                let allocation_status = allocation.status();
                let assigned_at = allocation.assigned_at();
                let job_status =
                    state
                        .jobs
                        .get(&job_id)
                        .map(Job::status)
                        .ok_or(StateStoreError::invariant(
                            "reserved allocation references a missing job",
                        ))?;
                let mut events = Vec::with_capacity(2);
                if job_status == JobStatus::Pending
                    && allocation_status == AllocationStatus::Assigned
                {
                    events.push(JobEvent::Assigned {
                        job_id,
                        allocation_id,
                        agent_id,
                        occurred_at: assigned_at,
                    });
                }
                events.push(JobEvent::Lost {
                    job_id,
                    allocation_id,
                    agent_id,
                    occurred_at: now,
                    reason: crate::domain::LostReason::HeartbeatExpired,
                });
                let outcome = apply_events_in_state(&mut state, &events)?;
                if outcome != ApplyOutcome::Applied {
                    return Err(StateStoreError::invariant(
                        "liveness events did not apply to an active allocation",
                    ));
                }
            } else {
                if agent.availability() != AgentAvailability::Idle {
                    return Err(StateStoreError::invariant(
                        "busy agent has no reservation during liveness sweep",
                    ));
                }
                if state.allocations.values().any(|allocation| {
                    allocation.agent_id() == agent_id
                        && !is_terminal_allocation(allocation.status())
                }) {
                    return Err(StateStoreError::invariant(
                        "active allocation exists without an agent reservation",
                    ));
                }
            }

            let agent = state
                .agents
                .get_mut(&agent_id)
                .ok_or(StateStoreError::invariant(
                    "liveness candidate is no longer registered",
                ))?;
            agent.mark_unhealthy();
            unhealthy_agents.push(agent_id);
        }

        Ok(unhealthy_agents)
    }
}

fn apply_events_in_state(
    state: &mut StoreState,
    events: &[JobEvent],
) -> Result<ApplyOutcome, StateStoreError> {
    let Some(first_event) = events.first() else {
        return Err(StateStoreError::invariant(
            "event transaction cannot be empty",
        ));
    };
    let job_id = event_job_id(first_event);
    let mut updated_job = state
        .jobs
        .get(&job_id)
        .cloned()
        .ok_or_else(|| StateStoreError::not_found("job", job_id))?;
    let old_status = updated_job.status();
    // Keep a context-free stop from bypassing an allocation created before the
    // corresponding Assigned event is applied.
    let allocation_id = event_allocation_id(first_event).or_else(|| match first_event {
        JobEvent::Stopped {
            allocation_id: None,
            ..
        } => state
            .allocations
            .values()
            .find(|allocation| allocation.job_id() == job_id)
            .map(|allocation| allocation.id()),
        _ => None,
    });
    let mut updated_allocation = allocation_id.and_then(|id| state.allocations.get(&id).cloned());

    let mut outcome = ApplyOutcome::Applied;
    for event in events {
        outcome = apply_event(&mut updated_job, updated_allocation.as_mut(), event)
            .map_err(StateStoreError::Transition)?;
        if outcome != ApplyOutcome::Applied {
            if events.len() == 1 {
                return Ok(outcome);
            }
            return Err(StateStoreError::invariant(
                "all events in a multi-event transaction must apply",
            ));
        }
    }

    let pending_count = pending_job_count(state, job_id);
    let pending_index = if old_status == JobStatus::Pending {
        if pending_count != 1 {
            return Err(StateStoreError::invariant(
                "pending job must appear exactly once in the pending queue",
            ));
        }
        state
            .pending_jobs
            .iter()
            .position(|pending_id| *pending_id == job_id)
    } else {
        if pending_count != 0 {
            return Err(StateStoreError::invariant(
                "non-pending job is still present in the pending queue",
            ));
        }
        None
    };

    let updated_agent = if let Some(allocation) = updated_allocation.as_ref() {
        let allocation_id = allocation.id();
        let agent_id = allocation.agent_id();
        if state.agent_allocations.get(&agent_id) != Some(&allocation_id) {
            return Err(StateStoreError::invariant(
                "event allocation does not match the agent reservation",
            ));
        }
        let mut agent = state
            .agents
            .get(&agent_id)
            .cloned()
            .ok_or(StateStoreError::invariant(
                "event allocation references an unregistered agent",
            ))?;
        if agent.availability() != AgentAvailability::Busy {
            return Err(StateStoreError::invariant(
                "reserved agent is not marked busy",
            ));
        }
        if is_terminal_allocation(allocation.status()) {
            agent.set_availability(AgentAvailability::Idle);
            Some((agent_id, agent))
        } else {
            None
        }
    } else {
        None
    };

    state.jobs.insert(job_id, updated_job);
    if let (Some(id), Some(allocation)) = (allocation_id, updated_allocation) {
        state.allocations.insert(id, allocation);
    }
    if let Some(index) = pending_index {
        let _ = state.pending_jobs.remove(index);
    }
    if let Some((agent_id, agent)) = updated_agent {
        state.agent_allocations.remove(&agent_id);
        state.agents.insert(agent_id, agent);
    }

    Ok(outcome)
}

fn heartbeat_expired(now: Timestamp, last_heartbeat_at: Timestamp, timeout: Duration) -> bool {
    Duration::from_millis(
        now.as_unix_millis()
            .saturating_sub(last_heartbeat_at.as_unix_millis()),
    ) >= timeout
}

fn pending_job_count(state: &StoreState, id: JobId) -> usize {
    state
        .pending_jobs
        .iter()
        .filter(|pending_id| **pending_id == id)
        .count()
}

fn is_terminal_allocation(status: AllocationStatus) -> bool {
    matches!(
        status,
        AllocationStatus::Succeeded
            | AllocationStatus::Failed
            | AllocationStatus::Stopped
            | AllocationStatus::Lost
    )
}

fn event_job_id(event: &JobEvent) -> JobId {
    match event {
        JobEvent::Submitted { job_id, .. }
        | JobEvent::Assigned { job_id, .. }
        | JobEvent::Started { job_id, .. }
        | JobEvent::Finished { job_id, .. }
        | JobEvent::Stopped { job_id, .. }
        | JobEvent::Lost { job_id, .. } => *job_id,
    }
}

fn event_allocation_id(event: &JobEvent) -> Option<AllocationId> {
    match event {
        JobEvent::Submitted { .. } => None,
        JobEvent::Assigned { allocation_id, .. }
        | JobEvent::Started { allocation_id, .. }
        | JobEvent::Finished { allocation_id, .. }
        | JobEvent::Lost { allocation_id, .. } => Some(*allocation_id),
        JobEvent::Stopped { allocation_id, .. } => *allocation_id,
    }
}

#[cfg(test)]
mod tests {
    use super::StateStore;
    use crate::domain::{
        Agent, AgentAvailability, AgentHealth, AgentId, Allocation, AllocationId, AllocationStatus,
        ApplyOutcome, Job, JobEvent, JobId, JobSpec, JobStatus, ProcessResult, StopReason,
        Timestamp, apply_event,
    };
    use crate::state::error::StateStoreError;
    use std::{
        collections::BTreeMap,
        num::NonZeroU32,
        sync::{Arc, Barrier},
        thread,
        time::Duration,
    };

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

    fn job(id: JobId, submitted_at: u64) -> Job {
        let spec = JobSpec::new(
            "echo",
            vec!["hello".into()],
            Some(BTreeMap::from([("MODE".into(), "test".into())])),
            None,
        )
        .expect("fixture job spec is valid");
        Job::new(id, spec, Timestamp::from_unix_millis(submitted_at))
    }

    fn agent(id: AgentId, registered_at: u64) -> Agent {
        Agent::new(
            id,
            "worker.example:9000".into(),
            Timestamp::from_unix_millis(registered_at),
        )
    }

    fn allocation(
        id: AllocationId,
        job_id: JobId,
        agent_id: AgentId,
        assigned_at: u64,
    ) -> Allocation {
        Allocation::new(
            id,
            job_id,
            agent_id,
            NonZeroU32::new(1).unwrap(),
            Timestamp::from_unix_millis(assigned_at),
        )
    }

    fn store_with_allocation() -> (StateStore, JobId, AllocationId, AgentId) {
        let store = StateStore::default();
        let job_id = job_id(1);
        let allocation_id = allocation_id(1);
        let agent_id = agent_id(1);
        store.create_job(job(job_id, 100)).unwrap();
        store.register_agent(agent(agent_id, 100)).unwrap();
        store.reserve_agent(agent_id, allocation_id).unwrap();
        store
            .create_allocation(allocation(allocation_id, job_id, agent_id, 101))
            .unwrap();
        (store, job_id, allocation_id, agent_id)
    }

    fn assign(
        store: &StateStore,
        job_id: JobId,
        allocation_id: AllocationId,
        agent_id: AgentId,
        at: u64,
    ) {
        store
            .append_event(&JobEvent::Assigned {
                job_id,
                allocation_id,
                agent_id,
                occurred_at: Timestamp::from_unix_millis(at),
            })
            .unwrap();
    }

    fn start(
        store: &StateStore,
        job_id: JobId,
        allocation_id: AllocationId,
        agent_id: AgentId,
        at: u64,
    ) {
        store
            .append_event(&JobEvent::Started {
                job_id,
                allocation_id,
                agent_id,
                occurred_at: Timestamp::from_unix_millis(at),
            })
            .unwrap();
    }

    #[test]
    fn new_store_starts_with_empty_collections() {
        let store = StateStore::default();
        let state = store.inner.lock().expect("new store lock is not poisoned");

        assert!(state.jobs.is_empty());
        assert!(state.agents.is_empty());
        assert!(state.allocations.is_empty());
        assert!(state.pending_jobs.is_empty());
        assert!(state.agent_allocations.is_empty());
    }

    #[test]
    fn cloned_store_handles_share_the_same_state() {
        fn assert_send_sync<T: Send + Sync>() {}

        assert_send_sync::<StateStore>();

        let store = StateStore::default();
        let clone = store.clone();

        assert!(Arc::ptr_eq(&store.inner, &clone.inner));
    }

    #[test]
    fn create_job_stores_job_and_adds_it_to_pending_queue() {
        let store = StateStore::default();
        let new_job = job(job_id(1), 100);

        store.create_job(new_job.clone()).unwrap();

        assert_eq!(store.get_job(new_job.id()).unwrap(), new_job);
        assert_eq!(store.list_jobs().unwrap(), vec![new_job.clone()]);
        let state = store.inner.lock().expect("store lock is not poisoned");
        assert_eq!(state.pending_jobs.as_slices().0, &[new_job.id()]);
    }

    #[test]
    fn create_job_rejects_duplicate_id_without_duplicate_queue_entry() {
        let store = StateStore::default();
        let new_job = job(job_id(1), 100);
        store.create_job(new_job.clone()).unwrap();

        assert!(store.create_job(new_job.clone()).is_err());
        assert_eq!(store.get_job(new_job.id()).unwrap(), new_job);
        let state = store.inner.lock().expect("store lock is not poisoned");
        assert_eq!(
            state.pending_jobs.iter().copied().collect::<Vec<_>>(),
            vec![job_id(1)]
        );
    }

    #[test]
    fn concurrent_duplicate_job_creation_stores_one_job_and_queue_entry() {
        let store = StateStore::default();
        let new_job = job(job_id(1), 100);
        let barrier = Arc::new(Barrier::new(3));
        let handles = [new_job.clone(), new_job.clone()].map(|candidate| {
            let store = store.clone();
            let barrier = Arc::clone(&barrier);
            thread::spawn(move || {
                barrier.wait();
                store.create_job(candidate)
            })
        });

        barrier.wait();
        let results = handles.map(|handle| handle.join().unwrap());

        assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
        assert_eq!(
            results
                .iter()
                .filter(|result| matches!(
                    result,
                    Err(StateStoreError::AlreadyExists {
                        resource: "job",
                        ..
                    })
                ))
                .count(),
            1
        );
        assert_eq!(store.list_jobs().unwrap(), vec![new_job.clone()]);
        let state = store.inner.lock().expect("store lock is not poisoned");
        assert_eq!(
            state.pending_jobs.iter().copied().collect::<Vec<_>>(),
            vec![new_job.id()]
        );
    }

    #[test]
    fn create_job_rejects_non_pending_job_without_storing_it() {
        let mut non_pending_job = job(job_id(1), 100);
        let mut allocation = Allocation::new(
            allocation_id(1),
            non_pending_job.id(),
            agent_id(1),
            NonZeroU32::new(1).unwrap(),
            Timestamp::from_unix_millis(101),
        );
        let job_id = non_pending_job.id();
        let allocation_id = allocation.id();
        let agent_id = allocation.agent_id();
        assert_eq!(
            apply_event(
                &mut non_pending_job,
                Some(&mut allocation),
                &JobEvent::Assigned {
                    job_id,
                    allocation_id,
                    agent_id,
                    occurred_at: Timestamp::from_unix_millis(101),
                },
            )
            .unwrap(),
            ApplyOutcome::Applied
        );

        let store = StateStore::default();
        assert!(store.create_job(non_pending_job.clone()).is_err());
        assert!(store.get_job(non_pending_job.id()).is_err());
        assert!(store.list_jobs().unwrap().is_empty());
    }

    #[test]
    fn list_jobs_orders_by_submission_timestamp_then_id() {
        let store = StateStore::default();
        let first_id = job_id(1);
        let second_id = job_id(2);
        let third_id = job_id(3);

        store.create_job(job(third_id, 200)).unwrap();
        store.create_job(job(second_id, 100)).unwrap();
        store.create_job(job(first_id, 100)).unwrap();

        let ordered_ids = store
            .list_jobs()
            .unwrap()
            .into_iter()
            .map(|stored_job| stored_job.id())
            .collect::<Vec<_>>();
        assert_eq!(ordered_ids, vec![first_id, second_id, third_id]);
    }

    #[test]
    fn register_agent_stores_agent_and_rejects_duplicate_id() {
        let store = StateStore::default();
        let new_agent = agent(agent_id(1), 100);
        store.register_agent(new_agent.clone()).unwrap();

        assert_eq!(store.get_agent(new_agent.id()).unwrap(), new_agent);
        assert!(store.register_agent(new_agent.clone()).is_err());
        assert_eq!(store.get_agent(new_agent.id()).unwrap(), new_agent);
    }

    #[test]
    fn heartbeat_updates_are_monotonic_and_require_a_registered_agent() {
        let store = StateStore::default();
        let id = agent_id(1);
        store.register_agent(agent(id, 100)).unwrap();

        store
            .update_heartbeat(id, Timestamp::from_unix_millis(200))
            .unwrap();
        store
            .update_heartbeat(id, Timestamp::from_unix_millis(200))
            .unwrap();
        assert!(
            store
                .update_heartbeat(id, Timestamp::from_unix_millis(199))
                .is_err()
        );

        assert_eq!(
            store.get_agent(id).unwrap().last_heartbeat_at(),
            Timestamp::from_unix_millis(200)
        );
        assert!(
            store
                .update_heartbeat(agent_id(2), Timestamp::from_unix_millis(300))
                .is_err()
        );
    }

    #[test]
    fn liveness_sweep_expires_unallocated_agent_at_timeout_boundary() {
        let store = StateStore::default();
        let id = agent_id(1);
        store.register_agent(agent(id, 1_000)).unwrap();
        let timeout = Duration::from_millis(300);

        assert!(
            store
                .sweep_liveness(Timestamp::from_unix_millis(1_299), timeout)
                .unwrap()
                .is_empty()
        );
        assert_eq!(store.get_agent(id).unwrap().health(), AgentHealth::Healthy);

        assert_eq!(
            store
                .sweep_liveness(Timestamp::from_unix_millis(1_300), timeout)
                .unwrap(),
            vec![id]
        );
        let expired = store.get_agent(id).unwrap();
        assert_eq!(expired.health(), AgentHealth::Unhealthy);
        assert_eq!(expired.availability(), AgentAvailability::Idle);
    }

    #[test]
    fn liveness_sweep_does_not_expire_a_future_heartbeat() {
        let store = StateStore::default();
        let id = agent_id(1);
        store.register_agent(agent(id, 200)).unwrap();

        assert!(
            store
                .sweep_liveness(Timestamp::from_unix_millis(199), Duration::from_millis(1))
                .unwrap()
                .is_empty()
        );
        assert_eq!(store.get_agent(id).unwrap().health(), AgentHealth::Healthy);
    }

    #[test]
    fn liveness_sweep_loses_reserved_allocation_before_assigned_event() {
        let (store, job_id, allocation_id, agent_id) = store_with_allocation();

        assert_eq!(store.get_job(job_id).unwrap().status(), JobStatus::Pending);
        assert_eq!(
            store.get_allocation(allocation_id).unwrap().status(),
            AllocationStatus::Assigned
        );
        assert_eq!(
            store
                .sweep_liveness(Timestamp::from_unix_millis(400), Duration::from_millis(300))
                .unwrap(),
            vec![agent_id]
        );

        assert_eq!(store.get_job(job_id).unwrap().status(), JobStatus::Lost);
        assert_eq!(
            store.get_allocation(allocation_id).unwrap().status(),
            AllocationStatus::Lost
        );
        let agent = store.get_agent(agent_id).unwrap();
        assert_eq!(agent.health(), AgentHealth::Unhealthy);
        assert_eq!(agent.availability(), AgentAvailability::Idle);
        let state = store.inner.lock().expect("store lock is not poisoned");
        assert!(!state.pending_jobs.contains(&job_id));
        assert!(!state.agent_allocations.contains_key(&agent_id));
    }

    #[test]
    fn liveness_sweep_loses_assigned_job_and_releases_agent_reservation() {
        let (store, job_id, allocation_id, agent_id) = store_with_allocation();
        assign(&store, job_id, allocation_id, agent_id, 110);

        assert_eq!(
            store
                .sweep_liveness(Timestamp::from_unix_millis(400), Duration::from_millis(300))
                .unwrap(),
            vec![agent_id]
        );

        let job = store.get_job(job_id).unwrap();
        let allocation = store.get_allocation(allocation_id).unwrap();
        let agent = store.get_agent(agent_id).unwrap();
        assert_eq!(job.status(), JobStatus::Lost);
        assert_eq!(
            job.terminal_result(),
            Some(crate::domain::TerminalResult::Lost)
        );
        assert_eq!(allocation.status(), AllocationStatus::Lost);
        assert_eq!(agent.health(), AgentHealth::Unhealthy);
        assert_eq!(agent.availability(), AgentAvailability::Idle);
        assert!(
            !store
                .inner
                .lock()
                .expect("store lock is not poisoned")
                .agent_allocations
                .contains_key(&agent_id)
        );

        assert!(
            store
                .sweep_liveness(Timestamp::from_unix_millis(700), Duration::from_millis(300))
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn liveness_sweep_loses_running_job_and_allocation() {
        let (store, job_id, allocation_id, agent_id) = store_with_allocation();
        assign(&store, job_id, allocation_id, agent_id, 110);
        start(&store, job_id, allocation_id, agent_id, 120);

        assert_eq!(
            store
                .sweep_liveness(Timestamp::from_unix_millis(400), Duration::from_millis(300))
                .unwrap(),
            vec![agent_id]
        );

        assert_eq!(store.get_job(job_id).unwrap().status(), JobStatus::Lost);
        assert_eq!(
            store.get_allocation(allocation_id).unwrap().status(),
            AllocationStatus::Lost
        );
        assert_eq!(
            store.get_agent(agent_id).unwrap().health(),
            AgentHealth::Unhealthy
        );
    }

    #[test]
    fn concurrent_duplicate_terminal_reports_apply_once_and_release_reservation() {
        let (store, job_id, allocation_id, agent_id) = store_with_allocation();
        assign(&store, job_id, allocation_id, agent_id, 110);
        start(&store, job_id, allocation_id, agent_id, 120);
        let event = JobEvent::Finished {
            job_id,
            allocation_id,
            agent_id,
            occurred_at: Timestamp::from_unix_millis(130),
            result: ProcessResult::Exited { exit_code: 0 },
        };
        let barrier = Arc::new(Barrier::new(3));
        let handles = [0, 1].map(|_| {
            let store = store.clone();
            let barrier = Arc::clone(&barrier);
            let event = event.clone();
            thread::spawn(move || {
                barrier.wait();
                store.append_event(&event)
            })
        });

        barrier.wait();
        let results = handles.map(|handle| handle.join().unwrap().unwrap());

        assert_eq!(
            results
                .iter()
                .filter(|outcome| **outcome == ApplyOutcome::Applied)
                .count(),
            1
        );
        assert_eq!(
            results
                .iter()
                .filter(|outcome| **outcome == ApplyOutcome::Duplicate)
                .count(),
            1
        );
        assert_eq!(
            store.get_job(job_id).unwrap().status(),
            JobStatus::Succeeded
        );
        assert_eq!(
            store.get_allocation(allocation_id).unwrap().status(),
            AllocationStatus::Succeeded
        );
        assert_eq!(
            store.get_agent(agent_id).unwrap().availability(),
            AgentAvailability::Idle
        );
        let state = store.inner.lock().expect("store lock is not poisoned");
        assert!(!state.agent_allocations.contains_key(&agent_id));
    }

    #[test]
    fn rejected_stale_lost_event_preserves_agent_job_allocation_and_reservation() {
        let (store, job_id, allocation_id, agent_id) = store_with_allocation();
        assign(&store, job_id, allocation_id, agent_id, 110);
        start(&store, job_id, allocation_id, agent_id, 200);
        let job_before = store.get_job(job_id).unwrap();
        let allocation_before = store.get_allocation(allocation_id).unwrap();
        let agent_before = store.get_agent(agent_id).unwrap();

        assert!(matches!(
            store.sweep_liveness(Timestamp::from_unix_millis(150), Duration::from_millis(50)),
            Err(StateStoreError::Transition(_))
        ));

        assert_eq!(store.get_job(job_id).unwrap(), job_before);
        assert_eq!(
            store.get_allocation(allocation_id).unwrap(),
            allocation_before
        );
        assert_eq!(store.get_agent(agent_id).unwrap(), agent_before);
        assert_eq!(
            store
                .inner
                .lock()
                .expect("store lock is not poisoned")
                .agent_allocations
                .get(&agent_id),
            Some(&allocation_id)
        );
    }

    #[test]
    fn reservation_is_shared_with_agent_availability_and_releases_exact_match() {
        let store = StateStore::default();
        let id = agent_id(1);
        let first_allocation_id = allocation_id(1);
        let second_allocation_id = allocation_id(2);
        store.register_agent(agent(id, 100)).unwrap();

        store.reserve_agent(id, first_allocation_id).unwrap();
        assert_eq!(
            store.get_agent(id).unwrap().availability(),
            AgentAvailability::Busy
        );
        {
            let state = store.inner.lock().expect("store lock is not poisoned");
            assert_eq!(state.agent_allocations.get(&id), Some(&first_allocation_id));
        }

        assert!(store.reserve_agent(id, second_allocation_id).is_err());
        assert!(store.release_agent(id, second_allocation_id).is_err());
        assert_eq!(
            store.get_agent(id).unwrap().availability(),
            AgentAvailability::Busy
        );

        store.release_agent(id, first_allocation_id).unwrap();
        assert_eq!(
            store.get_agent(id).unwrap().availability(),
            AgentAvailability::Idle
        );
        let state = store.inner.lock().expect("store lock is not poisoned");
        assert!(!state.agent_allocations.contains_key(&id));
    }

    #[test]
    fn concurrent_reservations_for_one_agent_choose_one_allocation() {
        let store = StateStore::default();
        let id = agent_id(1);
        let first_allocation_id = allocation_id(1);
        let second_allocation_id = allocation_id(2);
        store.register_agent(agent(id, 100)).unwrap();
        let barrier = Arc::new(Barrier::new(3));
        let handles = [first_allocation_id, second_allocation_id].map(|candidate| {
            let store = store.clone();
            let barrier = Arc::clone(&barrier);
            thread::spawn(move || {
                barrier.wait();
                store.reserve_agent(id, candidate)
            })
        });

        barrier.wait();
        let results = handles.map(|handle| handle.join().unwrap());

        assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
        assert_eq!(
            results
                .iter()
                .filter(|result| matches!(
                    result,
                    Err(StateStoreError::Conflict {
                        resource: "agent",
                        ..
                    })
                ))
                .count(),
            1
        );
        let state = store.inner.lock().expect("store lock is not poisoned");
        let reserved_allocation = state
            .agent_allocations
            .get(&id)
            .expect("one reservation must be recorded");
        let winning_allocation = if results[0].is_ok() {
            &first_allocation_id
        } else {
            &second_allocation_id
        };
        assert_eq!(reserved_allocation, winning_allocation);
        assert_eq!(state.agent_allocations.len(), 1);
        assert_eq!(
            state.agents.get(&id).unwrap().availability(),
            AgentAvailability::Busy
        );
        assert_eq!(
            state.agents.get(&id).unwrap().health(),
            AgentHealth::Healthy
        );
    }

    #[test]
    fn allocation_creation_requires_a_known_pending_job_and_matching_reservation() {
        let store = StateStore::default();
        let job_id = job_id(1);
        let agent_id = agent_id(1);
        let allocation_id = allocation_id(1);
        store.register_agent(agent(agent_id, 100)).unwrap();
        store.reserve_agent(agent_id, allocation_id).unwrap();

        let allocation = allocation(allocation_id, job_id, agent_id, 101);
        assert!(store.create_allocation(allocation.clone()).is_err());
        store.create_job(job(job_id, 100)).unwrap();
        store.create_allocation(allocation.clone()).unwrap();
        assert_eq!(store.get_allocation(allocation_id).unwrap(), allocation);
        assert!(store.create_allocation(allocation.clone()).is_err());
        assert!(store.release_agent(agent_id, allocation_id).is_err());

        assert_eq!(store.get_allocation(allocation_id).unwrap(), allocation);
        assert_eq!(
            store.get_agent(agent_id).unwrap().availability(),
            AgentAvailability::Busy
        );
    }

    #[test]
    fn get_allocation_for_job_returns_none_for_pending_job() {
        let store = StateStore::default();
        let id = job_id(1);
        let pending_job = job(id, 100);
        store.create_job(pending_job.clone()).unwrap();

        assert_eq!(store.get_allocation_for_job(id).unwrap(), None);
        assert_eq!(store.get_job(id).unwrap(), pending_job);
        assert!(
            store
                .list_jobs()
                .unwrap()
                .iter()
                .any(|stored| stored.id() == id)
        );
        let state = store.inner.lock().expect("store lock is not poisoned");
        assert!(state.allocations.is_empty());
        assert_eq!(
            state.pending_jobs.iter().copied().collect::<Vec<_>>(),
            vec![id]
        );
        assert!(state.agent_allocations.is_empty());
    }

    #[test]
    fn get_allocation_for_job_returns_linked_allocation() {
        let (store, id, allocation_id, agent_id) = store_with_allocation();
        let allocation = store.get_allocation(allocation_id).unwrap();
        let stored_job = store.get_job(id).unwrap();
        let linked = store.get_allocation_for_job(id).unwrap();

        assert_eq!(linked, Some(allocation.clone()));
        assert_eq!(store.get_job(id).unwrap(), stored_job);
        assert_eq!(store.get_allocation(allocation_id).unwrap(), allocation);
        let state = store.inner.lock().expect("store lock is not poisoned");
        assert_eq!(state.allocations.len(), 1);
        assert_eq!(state.agent_allocations.get(&agent_id), Some(&allocation_id));
        assert_eq!(
            state.agents.get(&agent_id).unwrap().availability(),
            AgentAvailability::Busy
        );
    }

    #[test]
    fn get_allocation_for_job_rejects_unknown_job() {
        let store = StateStore::default();

        let error = store.get_allocation_for_job(job_id(99)).unwrap_err();

        assert!(matches!(
            error,
            StateStoreError::NotFound {
                resource: "job",
                ..
            }
        ));
    }

    #[test]
    fn allocation_creation_rejects_non_pending_job_without_mutation() {
        let store = StateStore::default();
        let job_id = job_id(1);
        let agent_id = agent_id(1);
        let allocation_id = allocation_id(1);
        store.create_job(job(job_id, 100)).unwrap();
        store.register_agent(agent(agent_id, 100)).unwrap();
        store.reserve_agent(agent_id, allocation_id).unwrap();

        let stop = JobEvent::Stopped {
            job_id,
            allocation_id: None,
            agent_id: None,
            occurred_at: Timestamp::from_unix_millis(101),
            reason: StopReason::UserRequested,
        };
        assert_eq!(store.append_event(&stop).unwrap(), ApplyOutcome::Applied);

        let error = store
            .create_allocation(allocation(allocation_id, job_id, agent_id, 102))
            .unwrap_err();
        assert!(matches!(
            error,
            StateStoreError::Conflict {
                resource: "job",
                ..
            }
        ));
        assert_eq!(store.get_job(job_id).unwrap().status(), JobStatus::Stopped);
        assert!(store.get_allocation(allocation_id).is_err());
        let state = store.inner.lock().expect("store lock is not poisoned");
        assert!(state.pending_jobs.is_empty());
        assert_eq!(state.agent_allocations.get(&agent_id), Some(&allocation_id));
        assert_eq!(
            state.agents.get(&agent_id).unwrap().availability(),
            AgentAvailability::Busy
        );
    }

    #[test]
    fn allocation_creation_rejects_unknown_agent_without_mutation() {
        let store = StateStore::default();
        let job_id = job_id(1);
        let agent_id = agent_id(1);
        let allocation_id = allocation_id(1);
        store.create_job(job(job_id, 100)).unwrap();

        let error = store
            .create_allocation(allocation(allocation_id, job_id, agent_id, 101))
            .unwrap_err();
        assert!(matches!(
            error,
            StateStoreError::NotFound {
                resource: "agent",
                ..
            }
        ));
        assert!(store.get_allocation(allocation_id).is_err());
        let state = store.inner.lock().expect("store lock is not poisoned");
        assert_eq!(
            state.pending_jobs.iter().copied().collect::<Vec<_>>(),
            vec![job_id]
        );
        assert!(!state.agent_allocations.contains_key(&agent_id));
    }

    #[test]
    fn allocation_creation_rejects_reservation_for_another_id_without_mutation() {
        let store = StateStore::default();
        let job_id = job_id(1);
        let agent_id = agent_id(1);
        let reserved_id = allocation_id(1);
        let other_id = allocation_id(2);
        store.create_job(job(job_id, 100)).unwrap();
        store.register_agent(agent(agent_id, 100)).unwrap();
        store.reserve_agent(agent_id, reserved_id).unwrap();

        assert!(
            store
                .create_allocation(allocation(other_id, job_id, agent_id, 101))
                .is_err()
        );
        assert!(store.get_allocation(other_id).is_err());
        let state = store.inner.lock().expect("store lock is not poisoned");
        assert_eq!(state.agent_allocations.get(&agent_id), Some(&reserved_id));
        assert!(state.allocations.is_empty());
        assert_eq!(
            state.pending_jobs.iter().copied().collect::<Vec<_>>(),
            vec![job_id]
        );
    }

    #[test]
    fn only_one_allocation_can_be_created_for_a_pending_job() {
        let store = StateStore::default();
        let job_id = job_id(1);
        let first_agent_id = agent_id(1);
        let second_agent_id = agent_id(2);
        let first_allocation_id = allocation_id(1);
        let second_allocation_id = allocation_id(2);
        store.create_job(job(job_id, 100)).unwrap();
        store.register_agent(agent(first_agent_id, 100)).unwrap();
        store.register_agent(agent(second_agent_id, 100)).unwrap();
        store
            .reserve_agent(first_agent_id, first_allocation_id)
            .unwrap();
        store
            .create_allocation(allocation(first_allocation_id, job_id, first_agent_id, 101))
            .unwrap();
        store
            .reserve_agent(second_agent_id, second_allocation_id)
            .unwrap();

        assert!(
            store
                .create_allocation(allocation(
                    second_allocation_id,
                    job_id,
                    second_agent_id,
                    102,
                ))
                .is_err()
        );
        assert!(store.get_allocation(second_allocation_id).is_err());
        store
            .release_agent(second_agent_id, second_allocation_id)
            .unwrap();
        assert_eq!(
            store.get_allocation(first_allocation_id).unwrap().job_id(),
            job_id
        );
    }

    #[test]
    fn append_event_applies_assignment_start_and_terminal_result_atomically() {
        let (store, job_id, allocation_id, agent_id) = store_with_allocation();
        let assigned = JobEvent::Assigned {
            job_id,
            allocation_id,
            agent_id,
            occurred_at: Timestamp::from_unix_millis(101),
        };
        let started = JobEvent::Started {
            job_id,
            allocation_id,
            agent_id,
            occurred_at: Timestamp::from_unix_millis(102),
        };
        let finished = JobEvent::Finished {
            job_id,
            allocation_id,
            agent_id,
            occurred_at: Timestamp::from_unix_millis(103),
            result: ProcessResult::Exited { exit_code: 0 },
        };

        assert_eq!(
            store.append_event(&assigned).unwrap(),
            ApplyOutcome::Applied
        );
        assert_eq!(store.get_job(job_id).unwrap().status(), JobStatus::Assigned);
        assert_eq!(store.append_event(&started).unwrap(), ApplyOutcome::Applied);
        assert_eq!(store.get_job(job_id).unwrap().status(), JobStatus::Running);
        assert_eq!(
            store.get_allocation(allocation_id).unwrap().status(),
            AllocationStatus::Running
        );
        assert_eq!(
            store.append_event(&finished).unwrap(),
            ApplyOutcome::Applied
        );

        assert_eq!(
            store.get_job(job_id).unwrap().status(),
            JobStatus::Succeeded
        );
        assert_eq!(
            store.get_allocation(allocation_id).unwrap().status(),
            AllocationStatus::Succeeded
        );
        assert_eq!(
            store.get_agent(agent_id).unwrap().availability(),
            AgentAvailability::Idle
        );
        let state = store.inner.lock().expect("store lock is not poisoned");
        assert!(state.pending_jobs.is_empty());
        assert!(!state.agent_allocations.contains_key(&agent_id));
    }

    #[test]
    fn pending_stop_removes_job_from_queue() {
        let store = StateStore::default();
        let job_id = job_id(1);
        store.create_job(job(job_id, 100)).unwrap();
        let stopped = JobEvent::Stopped {
            job_id,
            allocation_id: None,
            agent_id: None,
            occurred_at: Timestamp::from_unix_millis(101),
            reason: StopReason::UserRequested,
        };

        assert_eq!(store.append_event(&stopped).unwrap(), ApplyOutcome::Applied);
        assert_eq!(store.get_job(job_id).unwrap().status(), JobStatus::Stopped);
        let state = store.inner.lock().expect("store lock is not poisoned");
        assert!(state.pending_jobs.is_empty());
    }

    #[test]
    fn submitted_event_is_noop_and_preserves_pending_queue() {
        let store = StateStore::default();
        let job_id = job_id(1);
        store.create_job(job(job_id, 100)).unwrap();
        let submitted = JobEvent::Submitted {
            job_id,
            occurred_at: Timestamp::from_unix_millis(100),
        };

        assert_eq!(store.append_event(&submitted).unwrap(), ApplyOutcome::Noop);
        assert_eq!(store.get_job(job_id).unwrap().status(), JobStatus::Pending);
        let state = store.inner.lock().expect("store lock is not poisoned");
        assert_eq!(
            state.pending_jobs.iter().copied().collect::<Vec<_>>(),
            vec![job_id]
        );
    }

    #[test]
    fn assignment_removes_only_its_job_from_pending_fifo() {
        let store = StateStore::default();
        let first_job_id = job_id(1);
        let second_job_id = job_id(2);
        let allocation_id = allocation_id(1);
        let agent_id = agent_id(1);
        store.create_job(job(first_job_id, 100)).unwrap();
        store.create_job(job(second_job_id, 101)).unwrap();
        store.register_agent(agent(agent_id, 100)).unwrap();
        store.reserve_agent(agent_id, allocation_id).unwrap();
        store
            .create_allocation(allocation(allocation_id, first_job_id, agent_id, 102))
            .unwrap();

        store
            .append_event(&JobEvent::Assigned {
                job_id: first_job_id,
                allocation_id,
                agent_id,
                occurred_at: Timestamp::from_unix_millis(102),
            })
            .unwrap();

        let state = store.inner.lock().expect("store lock is not poisoned");
        assert_eq!(
            state.pending_jobs.iter().copied().collect::<Vec<_>>(),
            vec![second_job_id]
        );
    }

    #[test]
    fn terminal_duplicate_does_not_change_new_reservation_or_snapshots() {
        let (store, job_id, original_allocation_id, agent_id) = store_with_allocation();
        let assigned_at = Timestamp::from_unix_millis(101);
        let started_at = Timestamp::from_unix_millis(102);
        let finished_at = Timestamp::from_unix_millis(103);
        store
            .append_event(&JobEvent::Assigned {
                job_id,
                allocation_id: original_allocation_id,
                agent_id,
                occurred_at: assigned_at,
            })
            .unwrap();
        store
            .append_event(&JobEvent::Started {
                job_id,
                allocation_id: original_allocation_id,
                agent_id,
                occurred_at: started_at,
            })
            .unwrap();
        let event = JobEvent::Finished {
            job_id,
            allocation_id: original_allocation_id,
            agent_id,
            occurred_at: finished_at,
            result: ProcessResult::Exited { exit_code: 0 },
        };
        store.append_event(&event).unwrap();
        let job_before = store.get_job(job_id).unwrap();
        let allocation_before = store.get_allocation(original_allocation_id).unwrap();
        let next_allocation_id = allocation_id(2);
        store.reserve_agent(agent_id, next_allocation_id).unwrap();
        let agent_before = store.get_agent(agent_id).unwrap();

        assert_eq!(store.append_event(&event).unwrap(), ApplyOutcome::Duplicate);
        assert_eq!(store.get_job(job_id).unwrap(), job_before);
        assert_eq!(
            store.get_allocation(original_allocation_id).unwrap(),
            allocation_before
        );
        assert_eq!(store.get_agent(agent_id).unwrap(), agent_before);
        let state = store.inner.lock().expect("store lock is not poisoned");
        assert_eq!(
            state.agent_allocations.get(&agent_id),
            Some(&next_allocation_id)
        );
    }

    #[test]
    fn rejected_stale_event_leaves_all_related_state_unchanged() {
        let (store, job_id, allocation_id, agent_id) = store_with_allocation();
        store
            .append_event(&JobEvent::Assigned {
                job_id,
                allocation_id,
                agent_id,
                occurred_at: Timestamp::from_unix_millis(101),
            })
            .unwrap();
        let job_before = store.get_job(job_id).unwrap();
        let allocation_before = store.get_allocation(allocation_id).unwrap();
        let agent_before = store.get_agent(agent_id).unwrap();
        let stale_started = JobEvent::Started {
            job_id,
            allocation_id,
            agent_id,
            occurred_at: Timestamp::from_unix_millis(100),
        };

        assert!(matches!(
            store.append_event(&stale_started),
            Err(StateStoreError::Transition(_))
        ));
        assert_eq!(store.get_job(job_id).unwrap(), job_before);
        assert_eq!(
            store.get_allocation(allocation_id).unwrap(),
            allocation_before
        );
        assert_eq!(store.get_agent(agent_id).unwrap(), agent_before);
        let state = store.inner.lock().expect("store lock is not poisoned");
        assert!(state.pending_jobs.is_empty());
        assert_eq!(state.agent_allocations.get(&agent_id), Some(&allocation_id));
    }

    #[test]
    fn broken_reservation_rejects_terminal_event_without_partial_update() {
        let (store, job_id, allocation_id, agent_id) = store_with_allocation();
        store
            .append_event(&JobEvent::Assigned {
                job_id,
                allocation_id,
                agent_id,
                occurred_at: Timestamp::from_unix_millis(101),
            })
            .unwrap();
        store
            .append_event(&JobEvent::Started {
                job_id,
                allocation_id,
                agent_id,
                occurred_at: Timestamp::from_unix_millis(102),
            })
            .unwrap();
        {
            let mut state = store.inner.lock().expect("store lock is not poisoned");
            state.agent_allocations.remove(&agent_id);
        }
        let job_before = store.get_job(job_id).unwrap();
        let allocation_before = store.get_allocation(allocation_id).unwrap();
        let finished = JobEvent::Finished {
            job_id,
            allocation_id,
            agent_id,
            occurred_at: Timestamp::from_unix_millis(103),
            result: ProcessResult::Exited { exit_code: 1 },
        };

        assert!(matches!(
            store.append_event(&finished),
            Err(StateStoreError::InvariantViolation { .. })
        ));
        assert_eq!(store.get_job(job_id).unwrap(), job_before);
        assert_eq!(
            store.get_allocation(allocation_id).unwrap(),
            allocation_before
        );
        assert_eq!(
            store.get_agent(agent_id).unwrap().availability(),
            AgentAvailability::Busy
        );
    }

    #[test]
    fn missing_allocation_event_is_rejected_without_changing_job_or_queue() {
        let store = StateStore::default();
        let job_id = job_id(1);
        store.create_job(job(job_id, 100)).unwrap();
        let started = JobEvent::Started {
            job_id,
            allocation_id: allocation_id(1),
            agent_id: agent_id(1),
            occurred_at: Timestamp::from_unix_millis(101),
        };

        assert!(matches!(
            store.append_event(&started),
            Err(StateStoreError::Transition(_))
        ));
        assert_eq!(store.get_job(job_id).unwrap().status(), JobStatus::Pending);
        let state = store.inner.lock().expect("store lock is not poisoned");
        assert_eq!(
            state.pending_jobs.iter().copied().collect::<Vec<_>>(),
            vec![job_id]
        );
    }

    #[test]
    fn poisoned_mutex_is_reported_without_recovering_state() {
        let store = StateStore::default();
        let other_handle = store.clone();
        let poison_result = thread::spawn(move || {
            let _guard = other_handle
                .inner
                .lock()
                .expect("fresh mutex is not poisoned");
            panic!("intentionally poison the store mutex");
        })
        .join();
        assert!(poison_result.is_err());

        assert_eq!(
            store.get_job(job_id(1)).unwrap_err(),
            StateStoreError::LockPoisoned
        );
    }

    #[test]
    fn reads_of_unknown_entities_return_errors() {
        let store = StateStore::default();

        assert!(store.get_job(job_id(1)).is_err());
        assert!(store.get_agent(agent_id(1)).is_err());
        assert!(store.get_allocation(allocation_id(1)).is_err());
    }
}
