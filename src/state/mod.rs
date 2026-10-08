//! In-memory control-plane state.
//!
//! A single lock protects every collection so later operations can update jobs,
//! allocations, reservations, and queue membership atomically. Store operations
//! must keep lock sections short and release the lock before async or external
//! work.

use crate::domain::{Agent, AgentId, Allocation, AllocationId, Job, JobId};
use std::{
    collections::{HashMap, VecDeque},
    sync::{Arc, Mutex},
};

#[cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "M3.1 introduces the container; M3.2 adds operations and wires it to services"
    )
)]
#[derive(Clone, Default)]
pub(crate) struct StateStore {
    inner: Arc<Mutex<StoreState>>,
}

#[cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "M3.1 defines the shared state layout; M3.2 starts using its collections"
    )
)]
#[derive(Default)]
struct StoreState {
    jobs: HashMap<JobId, Job>,
    agents: HashMap<AgentId, Agent>,
    allocations: HashMap<AllocationId, Allocation>,
    pending_jobs: VecDeque<JobId>,
    agent_allocations: HashMap<AgentId, AllocationId>,
}

#[cfg(test)]
mod tests {
    use super::StateStore;
    use std::sync::Arc;

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
}
