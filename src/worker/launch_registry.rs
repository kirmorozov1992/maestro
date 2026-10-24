use crate::domain::AllocationId;
use std::{
    collections::HashSet,
    sync::{Mutex, MutexGuard, PoisonError},
};

/// In-memory admission guard for one agent process lifetime.
///
/// M6.4's agent loop must claim an allocation before handing it to the executor.
/// The guard intentionally does not survive a process restart.
#[derive(Default)]
pub(crate) struct LaunchRegistry {
    claimed: Mutex<HashSet<AllocationId>>,
}

#[cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "M6.4 agent loop will claim allocations before execution"
    )
)]
impl LaunchRegistry {
    pub(crate) fn claim(
        &self,
        allocation_id: AllocationId,
    ) -> Result<bool, PoisonError<MutexGuard<'_, HashSet<AllocationId>>>> {
        self.claimed
            .lock()
            .map(|mut claimed| claimed.insert(allocation_id))
    }
}

#[cfg(test)]
mod tests {
    use super::LaunchRegistry;
    use crate::domain::AllocationId;

    #[test]
    fn allocation_can_be_claimed_for_launch_only_once_per_registry() {
        let registry = LaunchRegistry::default();
        let allocation_id = AllocationId::new();

        assert!(registry.claim(allocation_id).unwrap());
        assert!(!registry.claim(allocation_id).unwrap());
    }
}
