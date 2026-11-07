use super::*;
use crate::domain::{JobSpec, ProcessResult, StopReason};
use std::{
    num::NonZeroU32,
    sync::{Arc, Barrier},
    thread,
};

fn assigned() -> (StateStore, JobId, AllocationId, AgentId) {
    let store = StateStore::default();
    let job = JobId::new();
    let agent = AgentId::new();
    let allocation = AllocationId::new();
    let at = Timestamp::from_unix_millis(100);
    store
        .create_job(Job::new(
            job,
            JobSpec::new("fixture", vec![], None, None).unwrap(),
            at,
        ))
        .unwrap();
    store
        .register_agent(Agent::new(agent, "stop-worker".into(), at))
        .unwrap();
    store
        .assign_allocation(Allocation::new(
            allocation,
            job,
            agent,
            NonZeroU32::new(1).unwrap(),
            at,
        ))
        .unwrap();
    (store, job, allocation, agent)
}
fn started(store: &StateStore, job: JobId, allocation: AllocationId, agent: AgentId) {
    store
        .append_event(&JobEvent::Started {
            job_id: job,
            allocation_id: allocation,
            agent_id: agent,
            occurred_at: Timestamp::from_unix_millis(110),
        })
        .unwrap();
}
#[test]
fn stop_is_sticky_idempotent_and_retained_across_start_without_terminal_mutation() {
    let (store, job, allocation, agent) = assigned();
    assert_eq!(store.poll_stop(agent, allocation).unwrap(), None);
    let before = store.get_job(job).unwrap();
    store.request_stop(job, allocation, agent).unwrap();
    store.request_stop(job, allocation, agent).unwrap();
    assert_eq!(store.get_job(job).unwrap(), before);
    let command = store.poll_stop(agent, allocation).unwrap().unwrap();
    assert_eq!(
        (command.job_id, command.allocation_id, command.agent_id),
        (job, allocation, agent)
    );
    assert_eq!(store.poll_stop(agent, allocation).unwrap(), Some(command));
    started(&store, job, allocation, agent);
    assert_eq!(store.poll_stop(agent, allocation).unwrap(), Some(command));
    assert_eq!(store.inner.lock().unwrap().stop_requests.len(), 1);
}
#[test]
fn rejects_foreign_unknown_and_mismatched_targets_without_queue_mutation() {
    let (store, job, allocation, agent) = assigned();
    let foreign = AgentId::new();
    store
        .register_agent(Agent::new(
            foreign,
            "foreign".into(),
            Timestamp::from_unix_millis(100),
        ))
        .unwrap();
    assert!(store.poll_stop(foreign, allocation).is_err());
    assert!(store.poll_stop(AgentId::new(), allocation).is_err());
    assert!(store.poll_stop(agent, AllocationId::new()).is_err());
    assert!(store.request_stop(JobId::new(), allocation, agent).is_err());
    assert!(store.request_stop(job, allocation, foreign).is_err());
    assert!(store.request_stop(job, AllocationId::new(), agent).is_err());
    assert!(store.inner.lock().unwrap().stop_requests.is_empty());
}
#[test]
fn terminal_outcomes_clear_command_and_leave_later_reservation_untouched() {
    for expired in [false, true] {
        let (store, job, allocation, agent) = assigned();
        store.request_stop(job, allocation, agent).unwrap();
        if expired {
            store
                .expire_assignments(Timestamp::from_unix_millis(200), Duration::from_millis(50))
                .unwrap();
        } else {
            started(&store, job, allocation, agent);
            store
                .append_event(&JobEvent::Stopped {
                    job_id: job,
                    allocation_id: Some(allocation),
                    agent_id: Some(agent),
                    occurred_at: Timestamp::from_unix_millis(200),
                    reason: StopReason::UserRequested,
                })
                .unwrap();
        }
        assert!(store.inner.lock().unwrap().stop_requests.is_empty());
        assert_eq!(store.poll_stop(agent, allocation).unwrap(), None);
        assert!(store.request_stop(job, allocation, agent).is_err());
        let later_job = JobId::new();
        let later_allocation = AllocationId::new();
        store
            .create_job(Job::new(
                later_job,
                JobSpec::new("fixture", vec![], None, None).unwrap(),
                Timestamp::from_unix_millis(300),
            ))
            .unwrap();
        store
            .assign_allocation(Allocation::new(
                later_allocation,
                later_job,
                agent,
                NonZeroU32::new(1).unwrap(),
                Timestamp::from_unix_millis(300),
            ))
            .unwrap();
        assert_eq!(store.poll_stop(agent, allocation).unwrap(), None);
        assert_eq!(store.poll_stop(agent, later_allocation).unwrap(), None);
    }
}
#[test]
fn concurrent_stop_and_finish_cannot_leave_a_stale_command() {
    let (store, job, allocation, agent) = assigned();
    started(&store, job, allocation, agent);
    let barrier = Arc::new(Barrier::new(2));
    let other = store.clone();
    let sync = barrier.clone();
    let stop = thread::spawn(move || {
        sync.wait();
        other.request_stop(job, allocation, agent)
    });
    barrier.wait();
    store
        .append_event(&JobEvent::Finished {
            job_id: job,
            allocation_id: allocation,
            agent_id: agent,
            occurred_at: Timestamp::from_unix_millis(200),
            result: ProcessResult::Exited { exit_code: 0 },
        })
        .unwrap();
    let result = stop.join().unwrap();
    assert!(result.is_ok() || matches!(result, Err(StateStoreError::Conflict { .. })));
    assert!(store.inner.lock().unwrap().stop_requests.is_empty());
    assert_eq!(store.poll_stop(agent, allocation).unwrap(), None);
    assert_eq!(store.get_job(job).unwrap().status(), JobStatus::Succeeded);
}
